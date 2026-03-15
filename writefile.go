package main

import (
	"sync"

	"github.com/knusbaum/go9p/fs"
	"github.com/knusbaum/go9p/proto"
)

// ActionFile is a read/write file whose content is generated on demand and
// whose writes are executed by a handler when the file is closed.
//
// - onRead is called each time a fid opens the file for reading; its result
//   is cached for the lifetime of that fid.  May be nil (reads return empty).
// - onWrite is called when a fid that was opened for writing closes; its
//   argument is the accumulated write data.  May be nil (writes are ignored).
type ActionFile struct {
	mu       sync.Mutex
	stat     proto.Stat
	parent   fs.Dir
	readBufs map[uint64][]byte // cached read content per fid
	writeBuf map[uint64][]byte // accumulated write data per fid
	onRead   func() []byte
	onWrite  func([]byte)
}

func newActionFile(f *fs.FS, name string, perm uint32, onRead func() []byte, onWrite func([]byte)) *ActionFile {
	return &ActionFile{
		stat:     *f.NewStat(name, "nobody", "nobody", perm),
		readBufs: make(map[uint64][]byte),
		writeBuf: make(map[uint64][]byte),
		onRead:   onRead,
		onWrite:  onWrite,
	}
}

// FSNode interface.

func (a *ActionFile) Stat() proto.Stat {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.stat
}

func (a *ActionFile) WriteStat(_ *proto.Stat) error { return nil }

func (a *ActionFile) SetParent(d fs.Dir) {
	a.mu.Lock()
	a.parent = d
	a.mu.Unlock()
}

func (a *ActionFile) Parent() fs.Dir {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.parent
}

// File interface.

func (a *ActionFile) Open(fid uint64, omode proto.Mode) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	mode := omode & 3 // strip flags like Otrunc
	if mode == proto.Oread || mode == proto.Ordwr {
		if a.onRead != nil {
			a.readBufs[fid] = a.onRead()
		} else {
			a.readBufs[fid] = nil
		}
	}
	if mode == proto.Owrite || mode == proto.Ordwr {
		a.writeBuf[fid] = nil
	}
	return nil
}

func (a *ActionFile) Read(fid uint64, offset uint64, count uint64) ([]byte, error) {
	a.mu.Lock()
	data := a.readBufs[fid]
	a.mu.Unlock()
	if offset >= uint64(len(data)) {
		return []byte{}, nil
	}
	end := offset + count
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	return data[offset:end], nil
}

func (a *ActionFile) Write(fid uint64, offset uint64, data []byte) (uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Append regardless of offset (sequential writes from clients).
	a.writeBuf[fid] = append(a.writeBuf[fid], data...)
	return uint32(len(data)), nil
}

func (a *ActionFile) Close(fid uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if buf, ok := a.writeBuf[fid]; ok {
		if len(buf) > 0 && a.onWrite != nil {
			a.onWrite(buf)
		}
		delete(a.writeBuf, fid)
	}
	delete(a.readBufs, fid)
	return nil
}
