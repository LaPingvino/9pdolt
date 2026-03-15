package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// DoltFS holds a connection to the Dolt SQL server.
type DoltFS struct {
	db *sql.DB
}

// NewDoltFS opens a connection to the Dolt server using the given DSN.
// The DSN should not include a database name; branches are selected as databases.
func NewDoltFS(dsn string) (*DoltFS, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &DoltFS{db: db}, nil
}

// Branches returns a newline-separated list of branches (databases on the server).
func (d *DoltFS) Branches() ([]byte, error) {
	rows, err := d.db.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		// skip internal Dolt/MySQL databases
		switch name {
		case "information_schema", "mysql", "performance_schema", "sys":
			continue
		}
		out = append(out, name)
	}
	return []byte(strings.Join(out, "\n") + "\n"), nil
}

// Tables returns the table names for the given branch (database).
func (d *DoltFS) Tables(branch string) ([]string, error) {
	rows, err := d.db.Query("SHOW TABLES FROM `" + escapeName(branch) + "`")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, nil
}

// Schema returns the CREATE TABLE statement for the given table on the given branch.
func (d *DoltFS) Schema(branch, table string) ([]byte, error) {
	row := d.db.QueryRow(
		"SHOW CREATE TABLE `"+escapeName(branch)+"`.`"+escapeName(table)+"`",
	)
	var name, stmt string
	if err := row.Scan(&name, &stmt); err != nil {
		return nil, err
	}
	return []byte(stmt + "\n"), nil
}

// Data returns all rows of the given table as CSV.
func (d *DoltFS) Data(branch, table string) ([]byte, error) {
	rows, err := d.db.Query(
		"SELECT * FROM `" + escapeName(branch) + "`.`" + escapeName(table) + "`",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(cols)

	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		record := make([]string, len(cols))
		for i, v := range vals {
			if v == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", v)
			}
		}
		_ = w.Write(record)
	}
	w.Flush()
	return buf.Bytes(), nil
}

// branchNames returns just the slice of branch names.
func (d *DoltFS) branchNames() ([]string, error) {
	raw, err := d.Branches()
	if err != nil {
		return nil, err
	}
	s := strings.TrimRight(string(raw), "\n")
	if s == "" {
		return nil, nil
	}
	return strings.Split(s, "\n"), nil
}

// Query executes an arbitrary SELECT on the given branch and returns CSV.
func (d *DoltFS) Query(branch, query string) ([]byte, error) {
	// Switch to the branch database first via USE, then run the query.
	conn, err := d.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "USE `"+escapeName(branch)+"`"); err != nil {
		return nil, fmt.Errorf("USE %s: %w", branch, err)
	}
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(cols)

	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		record := make([]string, len(cols))
		for i, v := range vals {
			if v == nil {
				record[i] = ""
			} else {
				record[i] = fmt.Sprintf("%v", v)
			}
		}
		_ = w.Write(record)
	}
	w.Flush()
	return buf.Bytes(), nil
}

// Log returns the Dolt commit log for the given branch.
func (d *DoltFS) Log(branch string) ([]byte, error) {
	rows, err := d.db.Query(
		"SELECT commit_hash, committer, date, message FROM `" +
			escapeName(branch) + "`.dolt_log ORDER BY date DESC LIMIT 100",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var buf bytes.Buffer
	for rows.Next() {
		var hash, committer, date, message string
		if err := rows.Scan(&hash, &committer, &date, &message); err != nil {
			return nil, err
		}
		fmt.Fprintf(&buf, "commit %s\nAuthor: %s\nDate:   %s\n\n    %s\n\n",
			hash, committer, date, message)
	}
	return buf.Bytes(), nil
}

// escapeName escapes a MySQL identifier component (branch or table name).
// It replaces backticks to prevent injection.
func escapeName(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}
