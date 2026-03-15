# 9pdolt

Mounts a [Dolt](https://github.com/dolthub/dolt) database as a [9P](https://en.wikipedia.org/wiki/9P_(protocol)) filesystem.

## Usage

### Automatic (recommended)

**Without root** — uses FUSE:

```sh
9pdolt -repo /path/to/my-dolt-repo -fusemount ~/mnt/dolt
```

**With root** — uses the kernel v9fs driver:

```sh
sudo 9pdolt -repo /path/to/my-dolt-repo -mount /mnt/dolt
```

Press Ctrl-C (or send SIGTERM) to unmount, stop the server, and clean up.

### Manual

Start a Dolt SQL server separately, then run 9pdolt:

```sh
dolt sql-server                              # in your repo directory
9pdolt -dsn "root@tcp(localhost:3306)/" -mount /mnt/dolt
```

Or just expose the 9P server on TCP without mounting:

```sh
9pdolt -addr localhost:5640 -dsn "root@tcp(localhost:3306)/"
```

Then mount manually:

```sh
# Linux (v9fs, requires root):
mount -t 9p -o trans=tcp,port=5640,version=9p2000.L localhost /mnt/dolt

# Plan 9 / plan9port:
9pfuse tcp!localhost!5640 /mnt/dolt
```

## Filesystem layout

```
/
├── branches               ← newline-separated list of branches
└── db/
    └── <branch>/
        ├── log            ← commit log (last 100 entries)
        ├── status         ← dolt_status (staged/unstaged changes)
        ├── commit         ← write a message to commit; read back the hash
        ├── sql            ← write SQL to execute; read back the result
        ├── style          ← read/write: "csv", "json", or "file"
        └── tables/
            └── <table>/
                ├── schema ← CREATE TABLE statement
                └── ...    ← table data (format depends on style)
```

## Table styles

The `-style` flag (default `csv`) controls how table data appears.
You can also change it live per branch by writing to the `style` file.

### csv (default)

```
tables/<table>/
├── schema
└── data.csv    ← all rows as CSV; writable (REPLACE INTO on write)
```

### json

```
tables/<table>/
├── schema
├── 1.json      ← {"id":"1","name":"Alice"}
└── 2.json
```

### file

```
tables/<table>/
├── schema
└── 42/         ← one directory per row, named by primary key
    ├── id      ← contains "42\n"
    ├── name
    └── email
```

## Arbitrary SQL

Write any SQL to the `sql` file; read it back to get the result as CSV
(for SELECT) or `rows affected: N` (for DML):

```sh
echo "SELECT * FROM users LIMIT 5" > /mnt/dolt/db/main/sql
cat /mnt/dolt/db/main/sql
```

## Committing changes

```sh
echo "add new users" > /mnt/dolt/db/main/commit
cat /mnt/dolt/db/main/commit   # → abcdef1234...
```

## Changing style at runtime

```sh
echo json > /mnt/dolt/db/main/style
ls /mnt/dolt/db/main/tables/users/   # now shows .json files
```

## Flags

| Flag          | Default                      | Description                                                  |
|---------------|------------------------------|--------------------------------------------------------------|
| `-addr`       | `localhost:5640`             | TCP address to listen on (ignored when `-mount` is set)      |
| `-dsn`        | `root@tcp(localhost:3306)/`  | MySQL DSN for the Dolt server (ignored when `-repo` is set)  |
| `-repo`       |                              | Dolt repo path; auto-starts `dolt sql-server`                |
| `-mount`      |                              | Mountpoint; kernel v9fs via Unix socket (needs root)         |
| `-fusemount`  |                              | Mountpoint; FUSE bridge via Unix socket (no root needed)     |
| `-style`      | `csv`                        | Table style: `csv`, `json`, or `file`                        |
