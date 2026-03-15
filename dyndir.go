package main

import (
	"errors"
	"sync"

	"github.com/knusbaum/go9p/fs"
	"github.com/knusbaum/go9p/proto"
)

// DynDir is a read-only directory whose children are generated on each access.
type DynDir struct {
	stat     proto.Stat
	mu       sync.RWMutex
	parent   fs.Dir
	children func() map[string]fs.FSNode
}

func newDynDir(f *fs.FS, name string, children func() map[string]fs.FSNode) *DynDir {
	stat := *f.NewStat(name, "nobody", "nobody", 0555)
	stat.Mode |= proto.DMDIR
	stat.Qid.Qtype = uint8(stat.Mode >> 24)
	return &DynDir{stat: stat, children: children}
}

func (d *DynDir) Stat() proto.Stat {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.stat
}

func (d *DynDir) WriteStat(_ *proto.Stat) error {
	return errors.New("read only")
}

func (d *DynDir) SetParent(p fs.Dir) {
	d.mu.Lock()
	d.parent = p
	d.mu.Unlock()
}

func (d *DynDir) Parent() fs.Dir {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.parent
}

func (d *DynDir) Children() map[string]fs.FSNode {
	return d.children()
}
