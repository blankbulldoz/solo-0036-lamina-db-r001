package engine

import (
	"crypto/rand"
	"encoding/hex"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"time"
)

type staged struct {
	Mutation   versions.Mutation
	InsertOnly bool
}
type Transaction struct {
	ID        string `json:"id"`
	Snapshot  uint64 `json:"snapshot"`
	ReadOnly  bool   `json:"read_only"`
	Isolation string `json:"isolation"`
	Started   string `json:"started"`
	writes    map[string]map[string]staged
	reads     []ReadBasis
	seen      map[string]bool
	expired   string
}
type Commit struct {
	Sequence uint64 `json:"sequence"`
	Writes   int    `json:"writes"`
}

func (d *DB) Begin(readOnly bool, isolation string) (Transaction, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Transaction{}, err
	}
	if isolation == "" {
		isolation = "snapshot"
	}
	if isolation != "snapshot" && isolation != "serializable" {
		return Transaction{}, fault("invalid", "isolation must be snapshot or serializable")
	}
	if len(d.transactions) >= 128 {
		return Transaction{}, fault("capacity", "128 active transactions reached")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Transaction{}, err
	}
	tx := &Transaction{ID: hex.EncodeToString(id[:]), Snapshot: d.sequence, ReadOnly: readOnly, Isolation: isolation, Started: time.Now().UTC().Format(time.RFC3339Nano), writes: map[string]map[string]staged{}, seen: map[string]bool{}}
	d.transactions[tx.ID] = tx
	out := *tx
	out.writes = nil
	out.reads = nil
	out.seen = nil
	return out, nil
}
func (d *DB) tx(id string) (*Transaction, error) {
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	return tx, nil
}
func (d *DB) Put(id, table string, row datum.Row, insertOnly bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	if tx.expired != "" {
		return fault("capacity", "serializable transaction expired: %s", tx.expired)
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	normalized, err := r.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	key := r.Key(normalized)
	rows := d.store.Scan(table, tx.Snapshot)
	overlay(rows, tx.writes, table)
	if insertOnly {
		if _, ok := rows[key]; ok {
			return fault("conflict", "primary already exists")
		}
	}
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Row: normalized}, InsertOnly: insertOnly}
	return nil
}
func (tx *Transaction) reserve(table, key string) error {
	if _, exists := tx.writes[table][key]; exists {
		return nil
	}
	count := 0
	for _, m := range tx.writes {
		count += len(m)
	}
	if count >= 1000 {
		return fault("capacity", "transaction mutation limit 1000")
	}
	return nil
}
func (d *DB) Delete(id, table string, value datum.Value) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	if tx.ReadOnly {
		return fault("conflict", "read-only transaction")
	}
	if tx.expired != "" {
		return fault("capacity", "serializable transaction expired: %s", tx.expired)
	}
	r, err := d.catalog.Relation(table)
	if err != nil {
		return invalid(err)
	}
	if r.Created > tx.Snapshot {
		return fault("conflict", "relation outside transaction catalog snapshot")
	}
	col, _ := r.Column(r.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	key := value.Key()
	if tx.writes[table] == nil {
		tx.writes[table] = map[string]staged{}
	}
	if err = tx.reserve(table, key); err != nil {
		return err
	}
	tx.writes[table][key] = staged{Mutation: versions.Mutation{Relation: table, Key: key, Deleted: true}}
	return nil
}
func (d *DB) Rollback(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	if _, err := d.tx(id); err != nil {
		return err
	}
	delete(d.transactions, id)
	return nil
}
func (d *DB) Commit(id string) (Commit, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Commit{}, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return Commit{}, err
	}
	defer delete(d.transactions, id)
	if tx.expired != "" {
		return Commit{}, fault("capacity", "serializable transaction expired: %s", tx.expired)
	}
	writes := make([]versions.Mutation, 0)
	for table, mutations := range tx.writes {
		for key, m := range mutations {
			if d.store.Latest(table, key) > tx.Snapshot {
				d.conflicts++
				return Commit{}, fault("conflict", "write conflict on %s", table)
			}
			writes = append(writes, m.Mutation)
		}
		if err = d.unique(table, tx.writes); err != nil {
			d.conflicts++
			return Commit{}, err
		}
	}
	if len(writes) == 0 {
		return Commit{Sequence: tx.Snapshot}, nil
	}
	if tx.serializable() {
		if tx.Snapshot < d.horizon {
			d.serializationExpired++
			return Commit{}, fault("capacity", "serializable transaction expired: read basis no longer retained")
		}
		if err = d.validateReads(tx); err != nil {
			d.serializationConflicts++
			return Commit{}, err
		}
	}
	sort.Slice(writes, func(i, j int) bool {
		if writes[i].Relation == writes[j].Relation {
			return writes[i].Key < writes[j].Key
		}
		return writes[i].Relation < writes[j].Relation
	})
	record := journal.Record{Kind: "commit", Transaction: id, Writes: writes}
	if tx.serializable() {
		record.Isolation = "serializable"
	}
	if err = d.publish(record); err != nil {
		return Commit{}, err
	}
	d.commits++
	return Commit{Sequence: d.sequence, Writes: len(writes)}, nil
}
