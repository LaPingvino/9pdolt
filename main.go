package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/knusbaum/go9p"
	"github.com/knusbaum/go9p/fs"
)

const validStyles = "csv, json, file"

func isValidStyle(s string) bool {
	return s == "csv" || s == "json" || s == "file"
}

func main() {
	addr := flag.String("addr", "localhost:5640", "9P listen address (TCP); ignored when -mount is set")
	dsn := flag.String("dsn", "root@tcp(localhost:3306)/", "Dolt MySQL DSN (no database); ignored when -repo is set")
	repo := flag.String("repo", "", "path to a Dolt repository; starts dolt sql-server automatically")
	mount := flag.String("mount", "", "mountpoint: kernel v9fs via Unix socket (requires root)")
	fusemount := flag.String("fusemount", "", "mountpoint: FUSE bridge (no root required)")
	style := flag.String("style", "csv", "table style: csv (single file), json (rows as .json files), file (rows as dirs with per-column files)")
	flag.Parse()

	if !isValidStyle(*style) {
		fmt.Fprintf(os.Stderr, "unknown style %q: must be one of %s\n", *style, validStyles)
		os.Exit(1)
	}

	// cleanups run in reverse order on exit.
	var cleanups []func()
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}()
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

	f, _ := buildFS(dolt, *style)
	srv := f.Server()

	if *fusemount != "" {
		cleanup, err := fuseMount(*fusemount, srv)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to FUSE mount: %v\n", err)
			os.Exit(1)
		}
		cleanups = append(cleanups, cleanup)
		log.Printf("9pdolt FUSE mounted at %s", *fusemount)
		select {}
	}

	if *mount != "" {
		sockPath := fmt.Sprintf("/tmp/9pdolt-%d.sock", os.Getpid())
		cleanups = append(cleanups, func() { os.Remove(sockPath) })
		go func() {
			if err := serveUnix(sockPath, srv); err != nil {
				log.Printf("9P server error: %v", err)
			}
		}()
		waitForSocket(sockPath, 0)
		if err := mountFS(sockPath, *mount); err != nil {
			fmt.Fprintf(os.Stderr, "failed to mount: %v\n", err)
			os.Exit(1)
		}
		cleanups = append(cleanups, func() { unmountFS(*mount) })
		log.Printf("9pdolt mounted at %s (socket %s)", *mount, sockPath)
		select {}
	}

	log.Printf("9pdolt listening on %s", *addr)
	log.Fatal(go9p.Serve(*addr, srv))
}

func buildFS(dolt *DoltFS, defaultStyle string) (*fs.FS, *fs.StaticDir) {
	f, root := fs.NewFS("nobody", "nobody", 0555)

	root.AddChild(fs.NewDynamicFile(
		f.NewStat("branches", "nobody", "nobody", 0444),
		func() []byte {
			data, err := dolt.Branches()
			if err != nil {
				return errBytes(err)
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
			children[branch] = newBranchDir(f, dolt, branch, defaultStyle)
		}
		return children
	}))

	return f, root
}

func newBranchDir(f *fs.FS, dolt *DoltFS, branch, defaultStyle string) fs.FSNode {
	// Per-branch mutable style, initialised from the global default.
	var styleMu sync.Mutex
	currentStyle := defaultStyle
	getStyle := func() string {
		styleMu.Lock()
		defer styleMu.Unlock()
		return currentStyle
	}

	styleFile := newActionFile(f, "style", 0666,
		func() []byte { return []byte(getStyle() + "\n") },
		func(in []byte) {
			s := strings.TrimRight(string(in), "\n\r ")
			if isValidStyle(s) {
				styleMu.Lock()
				currentStyle = s
				styleMu.Unlock()
			}
		},
	)

	// sql: write SQL → execute; read → last result.
	var sqlMu sync.Mutex
	var sqlLast []byte
	sqlFile := newActionFile(f, "sql", 0666,
		func() []byte {
			sqlMu.Lock()
			defer sqlMu.Unlock()
			return sqlLast
		},
		func(in []byte) {
			query := strings.TrimRight(string(in), "\n\r ")
			result, err := dolt.ExecSQL(branch, query)
			sqlMu.Lock()
			if err != nil {
				sqlLast = errBytes(err)
			} else {
				sqlLast = result
			}
			sqlMu.Unlock()
		},
	)

	// commit: write message → DOLT_COMMIT; read → last hash.
	var commitMu sync.Mutex
	var commitLast []byte
	commitFile := newActionFile(f, "commit", 0666,
		func() []byte {
			commitMu.Lock()
			defer commitMu.Unlock()
			return commitLast
		},
		func(in []byte) {
			msg := strings.TrimRight(string(in), "\n\r ")
			result, err := dolt.Commit(branch, msg)
			commitMu.Lock()
			if err != nil {
				commitLast = errBytes(err)
			} else {
				commitLast = result
			}
			commitMu.Unlock()
		},
	)

	tablesDir := newDynDir(f, "tables", func() map[string]fs.FSNode {
		tables, err := dolt.Tables(branch)
		if err != nil {
			return nil
		}
		children := make(map[string]fs.FSNode, len(tables))
		for _, t := range tables {
			table := t
			children[table] = newTableDir(f, dolt, branch, table, getStyle())
		}
		return children
	})

	return newDynDir(f, branch, func() map[string]fs.FSNode {
		return map[string]fs.FSNode{
			"log": fs.NewDynamicFile(
				f.NewStat("log", "nobody", "nobody", 0444),
				func() []byte {
					data, err := dolt.Log(branch)
					if err != nil {
						return errBytes(err)
					}
					return data
				},
			),
			"status": fs.NewDynamicFile(
				f.NewStat("status", "nobody", "nobody", 0444),
				func() []byte {
					data, err := dolt.Status(branch)
					if err != nil {
						return errBytes(err)
					}
					return data
				},
			),
			"sql":    sqlFile,
			"commit": commitFile,
			"style":  styleFile,
			"tables": tablesDir,
		}
	})
}

func newTableDir(f *fs.FS, dolt *DoltFS, branch, table, style string) fs.FSNode {
	schemaFile := fs.NewDynamicFile(
		f.NewStat("schema", "nobody", "nobody", 0444),
		func() []byte {
			data, err := dolt.Schema(branch, table)
			if err != nil {
				return errBytes(err)
			}
			return data
		},
	)

	switch style {
	case "json":
		return newTableDirJSON(f, dolt, branch, table, schemaFile)
	case "file":
		return newTableDirFile(f, dolt, branch, table, schemaFile)
	default: // "csv"
		dataFile := newActionFile(f, "data.csv", 0666,
			func() []byte {
				data, err := dolt.Data(branch, table)
				if err != nil {
					return errBytes(err)
				}
				return data
			},
			func(in []byte) { _ = dolt.ReplaceCSV(branch, table, in) },
		)
		return newDynDir(f, table, func() map[string]fs.FSNode {
			return map[string]fs.FSNode{
				"schema":   schemaFile,
				"data.csv": dataFile,
			}
		})
	}
}

// newTableDirJSON: each row is a <pk>.json file.
func newTableDirJSON(f *fs.FS, dolt *DoltFS, branch, table string, schemaFile fs.FSNode) fs.FSNode {
	return newDynDir(f, table, func() map[string]fs.FSNode {
		rows, _, err := dolt.Rows(branch, table)
		if err != nil {
			return map[string]fs.FSNode{"schema": schemaFile}
		}
		pks, _ := dolt.PrimaryKey(branch, table)
		children := map[string]fs.FSNode{"schema": schemaFile}
		for _, row := range rows {
			row := row
			name := rowKey(row, pks) + ".json"
			children[name] = fs.NewDynamicFile(
				f.NewStat(name, "nobody", "nobody", 0444),
				func() []byte {
					b, _ := jsonMarshalRow(row)
					return b
				},
			)
		}
		return children
	})
}

// newTableDirFile: each row is a subdirectory named by PK, with one file per column.
func newTableDirFile(f *fs.FS, dolt *DoltFS, branch, table string, schemaFile fs.FSNode) fs.FSNode {
	return newDynDir(f, table, func() map[string]fs.FSNode {
		rows, cols, err := dolt.Rows(branch, table)
		if err != nil {
			return map[string]fs.FSNode{"schema": schemaFile}
		}
		pks, _ := dolt.PrimaryKey(branch, table)
		children := map[string]fs.FSNode{"schema": schemaFile}
		for _, row := range rows {
			row := row
			dirName := rowKey(row, pks)
			colFiles := make(map[string]fs.FSNode, len(cols))
			for _, col := range cols {
				col := col
				colFiles[col] = fs.NewStaticFile(
					f.NewStat(col, "nobody", "nobody", 0444),
					[]byte(row[col]+"\n"),
				)
			}
			children[dirName] = newDynDir(f, dirName, func() map[string]fs.FSNode {
				return colFiles
			})
		}
		return children
	})
}

// rowKey builds a filesystem-safe name from a row's primary key values.
func rowKey(row map[string]string, pks []string) string {
	if len(pks) == 0 {
		var parts []string
		for _, v := range row {
			parts = append(parts, v)
		}
		return strings.Join(parts, "_")
	}
	parts := make([]string, len(pks))
	for i, pk := range pks {
		parts[i] = strings.ReplaceAll(row[pk], "/", "_")
	}
	return strings.Join(parts, "_")
}

func jsonMarshalRow(row map[string]string) ([]byte, error) {
	b, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func errBytes(err error) []byte {
	return []byte("error: " + err.Error() + "\n")
}
