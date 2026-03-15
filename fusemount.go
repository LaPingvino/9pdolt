package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/knusbaum/go9p"
	goclient "github.com/knusbaum/go9p/client"
	"github.com/knusbaum/go9p/proto"
)

// fuseMount starts the 9P server srv on a temp Unix socket, connects a 9P
// client to it, then mounts a FUSE filesystem at mountpoint that proxies all
// calls through that client. It returns a cleanup function.
func fuseMount(mountpoint string, srv go9p.Srv) (cleanup func(), err error) {
	sockPath := fmt.Sprintf("/tmp/9pdolt-fuse-%d.sock", os.Getpid())

	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listen unix %s: %w", sockPath, err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go go9p.ServeReadWriter(conn, conn, srv)
		}
	}()

	conn, err := net.DialTimeout("unix", sockPath, 5*time.Second)
	if err != nil {
		ln.Close()
		os.Remove(sockPath)
		return nil, fmt.Errorf("dial 9P server: %w", err)
	}
	c9p, err := goclient.NewClient(conn, "nobody", "/")
	if err != nil {
		conn.Close()
		ln.Close()
		os.Remove(sockPath)
		return nil, fmt.Errorf("9P handshake: %w", err)
	}

	root := &p9Node{client: c9p, p9path: "/"}
	timeout := time.Second
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			Name:          "9pdolt",
			FsName:        "9pdolt",
			DisableXAttrs: true,
		},
		EntryTimeout: &timeout,
		AttrTimeout:  &timeout,
	})
	if err != nil {
		ln.Close()
		os.Remove(sockPath)
		return nil, fmt.Errorf("FUSE mount: %w", err)
	}

	go server.Wait()

	cleanup = func() {
		server.Unmount()
		ln.Close()
		os.Remove(sockPath)
	}
	return cleanup, nil
}

// p9Node is a FUSE inode that proxies to a 9P path via the client.
type p9Node struct {
	fs.Inode
	client *goclient.Client
	p9path string
}

var _ fs.NodeGetattrer = (*p9Node)(nil)
var _ fs.NodeLookuper = (*p9Node)(nil)
var _ fs.NodeReaddirer = (*p9Node)(nil)
var _ fs.NodeOpener = (*p9Node)(nil)
var _ fs.NodeCreater = (*p9Node)(nil)
var _ fs.NodeSetattrer = (*p9Node)(nil)

// Setattr is a no-op — ActionFile buffers are always per-open, so truncation
// is handled naturally when a new write session starts.
func (n *p9Node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return n.Getattr(ctx, f, out)
}

// Create is called for open-with-O_CREAT on existing 9P files (e.g. shell ">").
// We ignore creation flags and just open the file for writing.
func (n *p9Node) Create(ctx context.Context, name string, flags uint32, perm uint32, out *fuse.EntryOut) (node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	childPath := path.Join(n.p9path, name)
	f, err := n.client.Open(childPath, proto.Owrite)
	if err != nil {
		return nil, nil, 0, syscall.EIO
	}
	child := n.NewInode(ctx, &p9Node{client: n.client, p9path: childPath}, fs.StableAttr{})
	return child, &p9FileHandle{file: f}, fuse.FOPEN_DIRECT_IO, 0
}

func (n *p9Node) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	stat, err := n.client.Stat(n.p9path)
	if err != nil {
		return syscall.EIO
	}
	fillAttr(&out.Attr, stat)
	return 0
}

func (n *p9Node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	childPath := path.Join(n.p9path, name)
	stat, err := n.client.Stat(childPath)
	if err != nil {
		return nil, syscall.ENOENT
	}
	fillAttr(&out.Attr, stat)
	child := &p9Node{client: n.client, p9path: childPath}
	mode := fuse.S_IFREG
	if stat.Mode&proto.DMDIR != 0 {
		mode = fuse.S_IFDIR
	}
	return n.NewInode(ctx, child, fs.StableAttr{Mode: uint32(mode)}), 0
}

func (n *p9Node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, err := n.client.Readdir(n.p9path)
	if err != nil {
		return nil, syscall.EIO
	}
	dirEntries := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.Mode&proto.DMDIR != 0 {
			mode = syscall.S_IFDIR
		}
		dirEntries = append(dirEntries, fuse.DirEntry{Name: e.Name, Mode: mode})
	}
	return fs.NewListDirStream(dirEntries), 0
}

func (n *p9Node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	var p9mode proto.Mode
	switch flags & syscall.O_ACCMODE {
	case syscall.O_WRONLY:
		p9mode = proto.Owrite
	case syscall.O_RDWR:
		p9mode = proto.Ordwr
	default:
		p9mode = proto.Oread
	}
	f, err := n.client.Open(n.p9path, p9mode)
	if err != nil {
		return nil, 0, syscall.EIO
	}
	return &p9FileHandle{file: f}, fuse.FOPEN_DIRECT_IO, 0
}

// p9FileHandle proxies reads and writes through a 9P client File.
type p9FileHandle struct {
	file *goclient.File
}

var _ fs.FileReader = (*p9FileHandle)(nil)
var _ fs.FileWriter = (*p9FileHandle)(nil)
var _ fs.FileReleaser = (*p9FileHandle)(nil)

func (h *p9FileHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, err := h.file.ReadAt(dest, off)
	if err != nil && err != io.EOF && n == 0 {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *p9FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := h.file.WriteAt(data, off)
	if err != nil {
		return 0, syscall.EIO
	}
	return uint32(n), 0
}

func (h *p9FileHandle) Release(ctx context.Context) syscall.Errno {
	h.file.Close()
	return 0
}

func fillAttr(a *fuse.Attr, s *proto.Stat) {
	a.Mode = s.Mode & 0777
	if s.Mode&proto.DMDIR != 0 {
		a.Mode |= syscall.S_IFDIR
	} else {
		a.Mode |= syscall.S_IFREG
	}
	a.Size = s.Length
	a.Mtime = uint64(s.Mtime)
	a.Atime = uint64(s.Atime)
}
