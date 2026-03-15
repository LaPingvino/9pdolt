package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// startDoltServer starts "dolt sql-server" in repoDir, using a temp Unix
// socket. It returns the DSN to use and a cleanup function that stops the
// server and removes the socket file. The caller must call cleanup() when done.
func startDoltServer(repoDir string) (dsn string, cleanup func(), err error) {
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("9pdolt-%d.sock", os.Getpid()))

	cmd := exec.Command("dolt", "sql-server", "--socket", socketPath)
	cmd.Dir = repoDir
	cmd.Stdout = os.Stderr // route dolt logs to stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return "", nil, fmt.Errorf("starting dolt sql-server: %w", err)
	}

	cleanup = func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
		os.Remove(socketPath)
	}

	log.Printf("started dolt sql-server (pid %d), waiting for socket %s", cmd.Process.Pid, socketPath)

	if err := waitForSocket(socketPath, 15*time.Second); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("dolt sql-server did not become ready: %w", err)
	}

	dsn = "root@unix(" + socketPath + ")/"
	return dsn, cleanup, nil
}

// waitForSocket polls until the Unix socket at path is connectable or the
// timeout elapses.
func waitForSocket(path string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", path, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %s", timeout)
}
