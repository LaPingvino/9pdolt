# 9pdolt

Mounts a [Dolt](https://github.com/dolthub/dolt) database as a [9P](https://en.wikipedia.org/wiki/9P_(protocol)) filesystem.

## Usage

Start a Dolt SQL server first:

```sh
dolt sql-server
```

Then run 9pdolt:

```sh
9pdolt -addr localhost:5640 -dsn "root@tcp(localhost:3306)/"
```

Mount it (Linux example using `v9fs`):

```sh
mount -t 9p -o trans=tcp,port=5640,version=9p2000 localhost /mnt/dolt
```

Or on Plan 9 / with `9pfuse`:

```sh
9pfuse localhost:5640 /mnt/dolt
```

## Filesystem layout

```
/
├── branches          ← newline-separated list of branches (databases)
└── db/
    └── <branch>/
        ├── log           ← commit log (last 100 entries)
        ├── sql/          ← arbitrary SELECT queries (see below)
        └── <table>/
            ├── schema    ← CREATE TABLE statement
            └── data.csv  ← all rows as CSV
```

## Arbitrary SQL queries

Files under `sql/` are named with URL-encoded SQL. Reading them executes the
query and returns CSV:

```sh
# cat "/mnt/dolt/db/main/sql/SELECT * FROM users LIMIT 5" does not work
# because of spaces — use URL encoding:
cat "/mnt/dolt/db/main/sql/SELECT%20*%20FROM%20users%20LIMIT%205"
```

With Plan 9 tools:

```sh
cat /mnt/dolt/db/main/sql/SELECT+*+FROM+users+LIMIT+5
```

## Flags

| Flag    | Default                      | Description                              |
|---------|------------------------------|------------------------------------------|
| `-addr` | `localhost:5640`             | TCP address to listen on                 |
| `-dsn`  | `root@tcp(localhost:3306)/`  | MySQL DSN for the Dolt server (no DB)    |
