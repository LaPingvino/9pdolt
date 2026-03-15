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

// ExecSQL executes arbitrary SQL on the given branch. For SELECT it returns
// CSV; for DML it returns a "rows affected: N\n" line.
func (d *DoltFS) ExecSQL(branch, query string) ([]byte, error) {
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
		// Not a SELECT — try as a plain Exec for DML.
		res, err2 := conn.ExecContext(context.Background(), query)
		if err2 != nil {
			return nil, err // return the original query error
		}
		n, _ := res.RowsAffected()
		return []byte(fmt.Sprintf("rows affected: %d\n", n)), nil
	}
	defer rows.Close()
	return rowsToCSV(rows)
}

// Commit stages all changes and creates a Dolt commit with the given message.
// Returns the new commit hash.
func (d *DoltFS) Commit(branch, message string) ([]byte, error) {
	message = strings.TrimRight(message, "\n\r ")
	conn, err := d.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "USE `"+escapeName(branch)+"`"); err != nil {
		return nil, fmt.Errorf("USE %s: %w", branch, err)
	}
	rows, err := conn.QueryContext(context.Background(),
		"CALL DOLT_COMMIT('-Am', ?)", message)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			return nil, err
		}
		return []byte(hash + "\n"), nil
	}
	return []byte("ok\n"), nil
}

// Status returns the output of dolt_status for the given branch.
func (d *DoltFS) Status(branch string) ([]byte, error) {
	rows, err := d.db.Query(
		"SELECT table_name, staged, status FROM `" + escapeName(branch) + "`.dolt_status",
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var buf bytes.Buffer
	for rows.Next() {
		var table, staged, status string
		if err := rows.Scan(&table, &staged, &status); err != nil {
			return nil, err
		}
		stagedStr := "unstaged"
		if staged == "1" {
			stagedStr = "staged"
		}
		fmt.Fprintf(&buf, "%s\t%s\t%s\n", table, stagedStr, status)
	}
	if buf.Len() == 0 {
		return []byte("nothing to commit\n"), nil
	}
	return buf.Bytes(), nil
}

// ReplaceCSV imports CSV data into the given table using REPLACE INTO, which
// inserts new rows and updates existing rows matched by primary key.
// The first CSV row must be column names.
func (d *DoltFS) ReplaceCSV(branch, table string, data []byte) error {
	r := csv.NewReader(bytes.NewReader(data))
	records, err := r.ReadAll()
	if err != nil {
		return fmt.Errorf("parse CSV: %w", err)
	}
	if len(records) < 1 {
		return fmt.Errorf("CSV has no header row")
	}
	cols := records[0]
	if len(records) == 1 {
		return nil // header only, nothing to insert
	}

	conn, err := d.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "USE `"+escapeName(branch)+"`"); err != nil {
		return err
	}

	// Build REPLACE INTO `table` (`col1`,`col2`,...) VALUES (?,?,...)
	colPart := make([]string, len(cols))
	for i, c := range cols {
		colPart[i] = "`" + escapeName(c) + "`"
	}
	placeholders := "(" + strings.Repeat("?,", len(cols)-1) + "?)"
	stmt := fmt.Sprintf("REPLACE INTO `%s` (%s) VALUES %s",
		escapeName(table), strings.Join(colPart, ","), placeholders)

	prepared, err := conn.PrepareContext(context.Background(), stmt)
	if err != nil {
		return err
	}
	defer prepared.Close()

	for _, row := range records[1:] {
		if len(row) != len(cols) {
			continue // skip malformed rows
		}
		args := make([]interface{}, len(row))
		for i, v := range row {
			args[i] = v
		}
		if _, err := prepared.ExecContext(context.Background(), args...); err != nil {
			return fmt.Errorf("row %v: %w", row, err)
		}
	}
	return nil
}

// PrimaryKey returns the primary key column names for the given table.
func (d *DoltFS) PrimaryKey(branch, table string) ([]string, error) {
	rows, err := d.db.Query(
		`SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		 ORDER BY ORDINAL_POSITION`,
		branch, table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// Rows returns all rows of a table as a slice of maps (column → value string).
func (d *DoltFS) Rows(branch, table string) ([]map[string]string, []string, error) {
	sqlRows, err := d.db.Query(
		"SELECT * FROM `" + escapeName(branch) + "`.`" + escapeName(table) + "`",
	)
	if err != nil {
		return nil, nil, err
	}
	defer sqlRows.Close()
	cols, err := sqlRows.Columns()
	if err != nil {
		return nil, nil, err
	}
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	var result []map[string]string
	for sqlRows.Next() {
		if err := sqlRows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		row := make(map[string]string, len(cols))
		for i, v := range vals {
			row[cols[i]] = sqlValToString(v)
		}
		result = append(result, row)
	}
	return result, cols, nil
}

// sqlValToString converts a value scanned from database/sql into a string.
// The MySQL driver returns text/blob columns as []byte, not string.
func sqlValToString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch s := v.(type) {
	case []byte:
		return string(s)
	case string:
		return s
	default:
		return fmt.Sprintf("%v", s)
	}
}

// rowsToCSV serialises sql.Rows as CSV bytes.
func rowsToCSV(rows *sql.Rows) ([]byte, error) {
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
			record[i] = sqlValToString(v)
		}
		_ = w.Write(record)
	}
	w.Flush()
	return buf.Bytes(), nil
}

// escapeName escapes a MySQL identifier component (branch or table name).
// It replaces backticks to prevent injection.
func escapeName(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}
