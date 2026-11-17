package engine

import (
	"fmt"
	"lamina/internal/query"
	"lamina/internal/versions"
	"sort"
	"strings"
)

// maxSerializableReads bounds the distinct predicate read bases one
// serializable transaction retains for commit-time certification.
const maxSerializableReads = 64

// ReadBasis is one predicate read a serializable transaction performed at
// its snapshot. Commit revalidates that no commit after the snapshot
// changed the result of this read: a matched row was modified or deleted,
// or a row that did not match now matches (concurrent insert or update).
type ReadBasis struct {
	Relation string
	Where    []query.Predicate
}

// serializable reports whether the transaction certifies its read basis at
// commit. Read-only transactions publish nothing and are already serialized
// at their snapshot, so they need no certification.
func (tx *Transaction) serializable() bool {
	return tx.Isolation == "serializable" && !tx.ReadOnly
}

// expirable reports whether a forced vacuum may reclaim the versions this
// transaction's snapshot still pins. Only read-write serializable
// transactions depend on retained versions purely for certification and may
// be sacrificed with a resource rejection; snapshot and read-only
// transactions keep the baseline promise that their snapshots pin vacuum.
func (tx *Transaction) expirable() bool {
	return tx.serializable()
}

func (b ReadBasis) token() string {
	parts := make([]string, 0, len(b.Where))
	for _, p := range b.Where {
		parts = append(parts, p.Column+"\x00"+p.Op+"\x00"+p.Value.Key())
	}
	sort.Strings(parts)
	return b.Relation + "\x01" + strings.Join(parts, "\x01")
}

// recordRead retains one distinct read basis for commit-time certification.
// Exceeding the budget is a resource rejection, not a data conflict: the
// transaction is expired and every further operation fails with capacity.
func (d *DB) recordRead(tx *Transaction, relation string, where []query.Predicate) error {
	basis := ReadBasis{Relation: relation, Where: append([]query.Predicate(nil), where...)}
	token := basis.token()
	if tx.seen[token] {
		return nil
	}
	if len(tx.reads) >= maxSerializableReads {
		tx.expired = fmt.Sprintf("read basis budget %d exceeded", maxSerializableReads)
		d.serializationExpired++
		return fault("capacity", "serializable transaction expired: %s", tx.expired)
	}
	tx.seen[token] = true
	tx.reads = append(tx.reads, basis)
	return nil
}

// validateReads certifies that every recorded read basis still returns
// exactly what it returned at the transaction snapshot. For each key
// committed after the snapshot, a conflict exists when the key matched a
// basis at the snapshot (the row read is stale) or matches it now (a
// phantom entered the read). Keys untouched since the snapshot cannot
// affect any recorded read. The caller must hold d.mu and guarantee the
// snapshot is still inside the retained horizon.
func (d *DB) validateReads(tx *Transaction) error {
	for _, basis := range tx.reads {
		for _, chain := range d.store.Tables[basis.Relation] {
			if len(chain) == 0 || chain[len(chain)-1].Sequence <= tx.Snapshot {
				continue
			}
			before, existed := versions.Visible(chain, tx.Snapshot)
			if existed && query.Matches(before.Row, basis.Where) {
				return fault("conflict", "serialization conflict: read basis on %s changed by a concurrent commit", basis.Relation)
			}
			now, exists := versions.Visible(chain, d.sequence)
			if exists && query.Matches(now.Row, basis.Where) {
				return fault("conflict", "serialization conflict: read basis on %s changed by a concurrent commit", basis.Relation)
			}
		}
	}
	return nil
}
