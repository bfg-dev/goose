package goose

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// MigrationRecord struct.
type MigrationRecord struct {
	VersionID int64
	FileName  *string
	Note      *string
	TStamp    time.Time
	IsApplied bool // was this a result of up() or down()
	SQLData   *string
}

// Migration struct.
type Migration struct {
	Version    int64
	Next       int64  // next version, or -1 if none
	Previous   int64  // previous version, -1 if none
	Source     string // path to .sql script
	Note       *string
	Registered bool
	IsApplied  bool
	SQLData    *string
	TStamp     time.Time
	UpFn       func(*sql.Tx) error // Up go migration function
	DownFn     func(*sql.Tx) error // Down go migration function
}

func (m *Migration) String() string {
	return fmt.Sprintf(m.Source)
}

// Up runs an up migration.
func (m *Migration) Up(db *sql.DB, note string) error {
	if err := m.run(db, note, true); err != nil {
		return err
	}
	log.Println("OK   ", filepath.Base(m.Source))
	m.IsApplied = true
	return nil
}

// Down runs a down migration.
func (m *Migration) Down(db *sql.DB, note string) error {
	if err := m.run(db, note, false); err != nil {
		return err
	}
	log.Println("OK   ", filepath.Base(m.Source))
	m.IsApplied = false
	return nil
}

func (m *Migration) run(db *sql.DB, note string, direction bool) error {
	switch filepath.Ext(m.Source) {
	case ".sql":
		if err := runSQLMigration(db, m, note, direction); err != nil {
			return fmt.Errorf("FAIL %v, quitting migration", err)
		}

	case ".go":
		if !m.Registered {
			log.Fatalf("failed to apply Go migration %q: Go functions must be registered and built into a custom binary (see https://github.com/pressly/goose/tree/master/examples/go-migrations)", m.Source)
		}
		tx, err := db.Begin()
		if err != nil {
			log.Fatal("db.Begin: ", err)
		}

		fn := m.UpFn
		if !direction {
			fn = m.DownFn
		}
		if fn != nil {
			if err := fn(tx); err != nil {
				tx.Rollback()
				log.Fatalf("FAIL %s (%v), quitting migration.", filepath.Base(m.Source), err)
				return err
			}
		}
		if _, err := tx.Exec(GetDialect().insertVersionSQL(), m.Version, filepath.Base(m.Source), note, direction, nil); err != nil {
			tx.Rollback()
			return err
		}

		return tx.Commit()
	}

	return nil
}

// NumericComponent looks for migration scripts with names in the form:
// XXX_descriptivename.ext where XXX specifies the version number
// and ext specifies the type of migration
func NumericComponent(name string) (int64, error) {

	base := filepath.Base(name)

	if ext := filepath.Ext(base); ext != ".go" && ext != ".sql" {
		return 0, errors.New("not a recognized migration file type")
	}

	idx := strings.Index(base, "_")
	if idx < 0 {
		return 0, errors.New("no separator found")
	}

	n, e := strconv.ParseInt(base[:idx], 10, 64)
	if e == nil && n <= 0 {
		return 0, errors.New("migration IDs must be greater than zero")
	}

	return n, e
}
