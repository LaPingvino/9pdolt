package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"

	"github.com/knusbaum/go9p"
)

// serveUnix listens on a Unix socket and serves a 9P file system.
// Each accepted connection is handled in its own goroutine.
// The function blocks until the listener is closed or an accept error occurs.
func serveUnix(socketPath string, srv go9p.Srv) error {
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go go9p.ServeReadWriter(conn, conn, srv)
	}
}

// mountFS mounts the 9P server at socketPath onto mountpoint using the
// kernel's v9fs driver (trans=unix). Requires root or CAP_SYS_ADMIN.
func mountFS(socketPath, mountpoint string) error {
	cmd := exec.Command("mount",
		"-t", "9p",
		"-o", "trans=unix,version=9p2000.L,uname=nobody,access=any",
		socketPath, mountpoint,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mount: %w (are you root?)", err)
	}
	return nil
}

// unmountFS unmounts the given mountpoint.
func unmountFS(mountpoint string) {
	cmd := exec.Command("umount", mountpoint)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "umount %s: %v\n", mountpoint, err)
	}
}
