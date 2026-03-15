package main

// SQLQueryDir implements a directory of named SQL queries.
//
// Layout inside `sql/`:
//
//	sql/
//	├── myquery        ← write SQL here; read back the CSV result
//	└── otherquery     ← independent slot; persists until unlinked
//
// Creating a file (echo "SELECT ..." > sql/myquery) executes the query
// and stores the result.  Reading the file returns the last result.
// Removing the file (rm sql/myquery) drops the slot.

import (
	"sync"

	"github.com/knusbaum/go9p/fs"
	"github.com/knusbaum/go9p/proto"
)

// SQLQueryDir is a 9P directory node that manages per-name query slots.
// Each slot is a stable *ActionFile that persists until removed.
type SQLQueryDir struct {
	f      *fs.FS
	stat   proto.Stat
	parent fs.Dir
	exec   func(query string) []byte // executes SQL, returns CSV result bytes

	mu    sync.Mutex
	slots map[string]*ActionFile
}

func newSQLQueryDir(f *fs.FS, name string, exec func(string) []byte) *SQLQueryDir {
	stat := *f.NewStat(name, "nobody", "nobody", 0777)
	stat.Mode |= proto.DMDIR
	stat.Qid.Qtype = uint8(stat.Mode >> 24)
	return &SQLQueryDir{
		f:     f,
		stat:  stat,
		exec:  exec,
		slots: map[string]*ActionFile{},
	}
}

func (d *SQLQueryDir) Stat() proto.Stat {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stat
}

func (d *SQLQueryDir) WriteStat(_ *proto.Stat) error { return nil }

func (d *SQLQueryDir) SetParent(p fs.Dir) {
	d.mu.Lock()
	d.parent = p
	d.mu.Unlock()
}

func (d *SQLQueryDir) Parent() fs.Dir {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.parent
}

// Children returns all current query slots.
func (d *SQLQueryDir) Children() map[string]fs.FSNode {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]fs.FSNode, len(d.slots))
	for name, af := range d.slots {
		out[name] = af
	}
	return out
}

// newSlot creates a stable ActionFile for a new query slot.
func (d *SQLQueryDir) newSlot(name string) *ActionFile {
	var mu sync.Mutex
	var result []byte
	af := newActionFile(d.f, name, 0666,
		func() []byte {
			mu.Lock()
			defer mu.Unlock()
			return result
		},
		func(in []byte) {
			q := trim(in)
			res := d.exec(q)
			mu.Lock()
			result = res
			mu.Unlock()
		},
	)
	af.SetParent(d)
	return af
}

// Create adds a new query slot and returns its ActionFile.
func (d *SQLQueryDir) Create(name string, perm uint32) (fs.File, error) {
	d.mu.Lock()
	af, exists := d.slots[name]
	if !exists {
		af = d.newSlot(name)
		d.slots[name] = af
	}
	d.mu.Unlock()
	return af, nil
}

// Remove drops the named slot.
func (d *SQLQueryDir) Remove(name string) error {
	d.mu.Lock()
	delete(d.slots, name)
	d.mu.Unlock()
	return nil
}

func trim(b []byte) string {
	s := string(b)
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ') {
		s = s[:len(s)-1]
	}
	return s
}
