# 9pdolt

Mounts a [Dolt](https://github.com/dolthub/dolt) database as a [9P](https://en.wikipedia.org/wiki/9P_(protocol)) filesystem.

## Usage

### Automatic (recommended)

Point `-repo` at a Dolt repository and `-mount` at an empty directory.
9pdolt will start `dolt sql-server`, serve 9P on a temp Unix socket, and
mount it — all in one step:

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

| Flag     | Default                      | Description                                              |
|----------|------------------------------|----------------------------------------------------------|
| `-addr`  | `localhost:5640`             | TCP address to listen on (ignored when `-mount` is set)  |
| `-dsn`   | `root@tcp(localhost:3306)/`  | MySQL DSN for the Dolt server (ignored when `-repo` set) |
| `-repo`  |                              | Dolt repo path; auto-starts `dolt sql-server`            |
| `-mount` |                              | Mountpoint; serves via Unix socket and mounts (needs root)|
