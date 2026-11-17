package index

import (
	"lamina/internal/datum"
	"sort"
)

type Entry struct {
	Value datum.Value
	Key   string
}
type Tree struct {
	entries []Entry
	seen    map[string]bool
}

func New() *Tree { return &Tree{seen: map[string]bool{}} }
func (t *Tree) Add(value datum.Value, key string) {
	if value.Null() {
		return
	}
	token := value.Key() + "\x00" + key
	if t.seen[token] {
		return
	}
	t.seen[token] = true
	at := sort.Search(len(t.entries), func(i int) bool {
		n := datum.Order(t.entries[i].Value, value)
		return n > 0 || (n == 0 && t.entries[i].Key >= key)
	})
	t.entries = append(t.entries, Entry{})
	copy(t.entries[at+1:], t.entries[at:])
	t.entries[at] = Entry{value, key}
}
func (t *Tree) Candidates(op string, value datum.Value) []string {
	if value.Null() {
		return nil
	}
	lo := sort.Search(len(t.entries), func(i int) bool { return datum.Order(t.entries[i].Value, value) >= 0 })
	hi := sort.Search(len(t.entries), func(i int) bool { return datum.Order(t.entries[i].Value, value) > 0 })
	begin, end := 0, len(t.entries)
	switch op {
	case "=":
		begin, end = lo, hi
	case ">":
		begin = hi
	case ">=":
		begin = lo
	case "<":
		end = lo
	case "<=":
		end = hi
	default:
		return nil
	}
	out := make([]string, 0, end-begin)
	seen := map[string]bool{}
	for _, e := range t.entries[begin:end] {
		if !seen[e.Key] {
			seen[e.Key] = true
			out = append(out, e.Key)
		}
	}
	return out
}
func (t *Tree) Size() int { return len(t.entries) }
