package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/knusbaum/go9p"
	"github.com/knusbaum/go9p/fs"
)

func main() {
	addr := flag.String("addr", "localhost:5640", "9P listen address (TCP); ignored when -mount is set")
	dsn := flag.String("dsn", "root@tcp(localhost:3306)/", "Dolt MySQL DSN (no database); ignored when -repo is set")
	repo := flag.String("repo", "", "path to a Dolt repository; starts dolt sql-server automatically")
	mount := flag.String("mount", "", "mountpoint: serve on a Unix socket and mount 9P there (requires root)")
	flag.Parse()

	// cleanups are run in reverse order on exit.
	var cleanups []func()
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}()

	// Single signal handler that drains cleanups then exits.
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
		os.Exit(0)
	}()

	if *repo != "" {
		socketDSN, cleanup, err := startDoltServer(*repo)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to start Dolt server: %v\n", err)
			os.Exit(1)
		}
		cleanups = append(cleanups, cleanup)
		*dsn = socketDSN
	}

	dolt, err := NewDoltFS(*dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to Dolt: %v\n", err)
		os.Exit(1)
	}

	f, _ := buildFS(dolt)

	srv := f.Server()

	if *mount != "" {
		// Use a per-process temp socket for the 9P server.
		sockPath := fmt.Sprintf("/tmp/9pdolt-%d.sock", os.Getpid())
		cleanups = append(cleanups, func() { os.Remove(sockPath) })

		go func() {
			if err := serveUnix(sockPath, srv); err != nil {
				log.Printf("9P server error: %v", err)
			}
		}()

		// Give the listener a moment to create the socket file.
		waitForSocket(sockPath, 0) // reuse existing helper, near-instant

		if err := mountFS(sockPath, *mount); err != nil {
			fmt.Fprintf(os.Stderr, "failed to mount: %v\n", err)
			os.Exit(1)
		}
		cleanups = append(cleanups, func() { unmountFS(*mount) })
		log.Printf("9pdolt mounted at %s (socket %s)", *mount, sockPath)

		// Block until signal (handled above).
		select {}
	}

	log.Printf("9pdolt listening on %s", *addr)
	log.Fatal(go9p.Serve(*addr, srv))
}

func buildFS(dolt *DoltFS) (*fs.FS, *fs.StaticDir) {
	f, root := fs.NewFS("nobody", "nobody", 0555)

	// Registry mapping sql DynDir → branch name for WalkFail to intercept.
	var sqlDirs sync.Map

	// WalkFail: intercept walks into sql dirs to create on-the-fly query files.
	f.WalkFail = func(fsys *fs.FS, parent fs.Dir, name string) (fs.FSNode, error) {
		branch, ok := sqlDirs.Load(parent)
		if !ok {
			return nil, fmt.Errorf("%s: not found", name)
		}
		query, err := url.QueryUnescape(name)
		if err != nil {
			return nil, fmt.Errorf("bad query encoding %q: %w", name, err)
		}
		b := branch.(string)
		return fs.NewDynamicFile(
			fsys.NewStat(name, "nobody", "nobody", 0444),
			func() []byte {
				data, err := dolt.Query(b, query)
				if err != nil {
					return []byte("error: " + err.Error() + "\n")
				}
				return data
			},
		), nil
	}

	root.AddChild(fs.NewDynamicFile(
		f.NewStat("branches", "nobody", "nobody", 0444),
		func() []byte {
			data, err := dolt.Branches()
			if err != nil {
				return []byte("error: " + err.Error() + "\n")
			}
			return data
		},
	))

	root.AddChild(newDynDir(f, "db", func() map[string]fs.FSNode {
		branches, err := dolt.branchNames()
		if err != nil {
			return nil
		}
		children := make(map[string]fs.FSNode, len(branches))
		for _, b := range branches {
			branch := b
			children[branch] = newBranchDir(f, dolt, branch, &sqlDirs)
		}
		return children
	}))

	return f, root
}

func newBranchDir(f *fs.FS, dolt *DoltFS, branch string, sqlDirs *sync.Map) fs.FSNode {
	return newDynDir(f, branch, func() map[string]fs.FSNode {
		children := map[string]fs.FSNode{
			"log": fs.NewDynamicFile(
				f.NewStat("log", "nobody", "nobody", 0444),
				func() []byte {
					data, err := dolt.Log(branch)
					if err != nil {
						return []byte("error: " + err.Error() + "\n")
					}
					return data
				},
			),
		}
		sqlDir := newDynDir(f, "sql", func() map[string]fs.FSNode {
			return map[string]fs.FSNode{}
		})
		sqlDirs.Store(sqlDir, branch)
		children["sql"] = sqlDir

		tables, err := dolt.Tables(branch)
		if err != nil {
			return children
		}
		for _, t := range tables {
			table := t
			children[table] = newTableDir(f, dolt, branch, table)
		}
		return children
	})
}

func newTableDir(f *fs.FS, dolt *DoltFS, branch, table string) fs.FSNode {
	return newDynDir(f, table, func() map[string]fs.FSNode {
		return map[string]fs.FSNode{
			"schema": fs.NewDynamicFile(
				f.NewStat("schema", "nobody", "nobody", 0444),
				func() []byte {
					data, err := dolt.Schema(branch, table)
					if err != nil {
						return []byte("error: " + err.Error() + "\n")
					}
					return data
				},
			),
			"data.csv": fs.NewDynamicFile(
				f.NewStat("data.csv", "nobody", "nobody", 0444),
				func() []byte {
					data, err := dolt.Data(branch, table)
					if err != nil {
						return []byte("error: " + err.Error() + "\n")
					}
					return data
				},
			),
		}
	})
}
