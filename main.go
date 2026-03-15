package main

import (
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"sync"

	"github.com/knusbaum/go9p"
	"github.com/knusbaum/go9p/fs"
)

func main() {
	addr := flag.String("addr", "localhost:5640", "9P listen address")
	dsn := flag.String("dsn", "root@tcp(localhost:3306)/", "Dolt MySQL DSN (no database)")
	flag.Parse()

	dolt, err := NewDoltFS(*dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to Dolt: %v\n", err)
		os.Exit(1)
	}

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
		file := fs.NewDynamicFile(
			fsys.NewStat(name, "nobody", "nobody", 0444),
			func() []byte {
				data, err := dolt.Query(b, query)
				if err != nil {
					return []byte("error: " + err.Error() + "\n")
				}
				return data
			},
		)
		return file, nil
	}

	// branches file at root.
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

	// db/ dir: each branch is a subdirectory.
	dbDir := newDynDir(f, "db", func() map[string]fs.FSNode {
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
	})
	root.AddChild(dbDir)

	log.Printf("9pdolt listening on %s", *addr)
	log.Fatal(go9p.Serve(*addr, f.Server()))
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
		// sql/ directory — WalkFail handles actual query files.
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
