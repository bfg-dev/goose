package goose

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

var (
	// ErrNoCurrentVersion when a current migration version is not found.
	ErrNoCurrentVersion = errors.New("no current version found")
	// ErrNoNextVersion when the next migration version is not found.
	ErrNoNextVersion = errors.New("no next version found")
	// ErrNoPendingVersion when the pending migration version is not found.
	ErrNoPendingVersion = errors.New("no pending version found")
	// MaxVersion is the maximum allowed version.
	MaxVersion int64 = 9223372036854775807 // max(int64)

	registeredGoMigrations = map[int64]*Migration{}
)

// Migrations slice.
type Migrations []*Migration

// helpers so we can use pkg sort
func (ms Migrations) Len() int      { return len(ms) }
func (ms Migrations) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
func (ms Migrations) Less(i, j int) bool {
	if ms[i].Version == ms[j].Version {
		log.Fatalf("goose: duplicate version %v detected:\n%v\n%v", ms[i].Version, ms[i].Source, ms[j].Source)
	}
	return ms[i].Version < ms[j].Version
}

// Current gets the current migration.
func (ms Migrations) Current(current int64) (*Migration, error) {
	for i, migration := range ms {
		if migration.Version == current {
			return ms[i], nil
		}
	}

	return nil, ErrNoCurrentVersion
}

// Next gets the next migration.
func (ms Migrations) Next(current int64) (*Migration, error) {
	for i, migration := range ms {
		if migration.Version > current && !migration.IsApplied {
			return ms[i], nil
		}
	}

	return nil, ErrNoNextVersion
}

// Previous : Get the previous migration.
func (ms Migrations) Previous(current int64) (*Migration, error) {
	for i := len(ms) - 1; i >= 0; i-- {
		if ms[i].Version < current {
			return ms[i], nil
		}
	}

	return nil, ErrNoNextVersion
}

// Last gets the last migration.
func (ms Migrations) Last() (*Migration, error) {
	if len(ms) == 0 {
		return nil, ErrNoNextVersion
	}

	return ms[len(ms)-1], nil
}

// FirstPending finds first unapplied
func (ms Migrations) FirstPending() (*Migration, error) {
	for i, migration := range ms {
		if !migration.IsApplied {
			return ms[i], nil
		}
	}

	return nil, ErrNoPendingVersion
}

func (ms Migrations) String() string {
	str := ""
	for _, m := range ms {
		str += fmt.Sprintln(m)
	}
	return str
}

// AddMigration adds a migration.
func AddMigration(up func(*sql.Tx) error, down func(*sql.Tx) error) {
	_, filename, _, _ := runtime.Caller(1)
	AddNamedMigration(filename, up, down)
}

// AddNamedMigration : Add a named migration.
func AddNamedMigration(filename string, up func(*sql.Tx) error, down func(*sql.Tx) error) {
	v, _ := NumericComponent(filename)
	migration := &Migration{Version: v, Next: -1, Previous: -1, Registered: true, UpFn: up, DownFn: down, Source: filename}

	if existing, ok := registeredGoMigrations[v]; ok {
		panic(fmt.Sprintf("failed to add migration %q: version conflicts with %q", filename, existing.Source))
	}

	registeredGoMigrations[v] = migration
}

// CollectMigrations returns all the valid looking migration scripts in the
// migrations folder and go func registry, and key them by version.
func CollectMigrations(db *sql.DB, dirpath string, current, target int64, forUp bool) (Migrations, error) {
	if _, err := os.Stat(dirpath); os.IsNotExist(err) {
		return nil, fmt.Errorf("%s directory does not exists", dirpath)
	}

	var migrations Migrations

	// SQL migration files.
	sqlMigrationFiles, err := filepath.Glob(dirpath + "/**.sql")
	if err != nil {
		return nil, err
	}
	for _, file := range sqlMigrationFiles {
		v, err := NumericComponent(file)
		if err != nil {
			return nil, err
		}
		if versionFilter(v, current, target) {
			migration := &Migration{Version: v, Next: -1, Previous: -1, Source: file}
			migrations = append(migrations, migration)
		}
	}

	// Go migrations registered via goose.AddMigration().
	for _, migration := range registeredGoMigrations {
		v, err := NumericComponent(migration.Source)
		if err != nil {
			return nil, err
		}
		if versionFilter(v, current, target) {
			migrations = append(migrations, migration)
		}
	}

	// Go migration files
	goMigrationFiles, err := filepath.Glob(dirpath + "/**.go")
	if err != nil {
		return nil, err
	}
	for _, file := range goMigrationFiles {
		v, err := NumericComponent(file)
		if err != nil {
			continue // Skip any files that don't have version prefix.
		}

		// Skip migrations already existing migrations registered via goose.AddMigration().
		if _, ok := registeredGoMigrations[v]; ok {
			continue
		}

		if versionFilter(v, current, target) {
			migration := &Migration{Version: v, Next: -1, Previous: -1, Source: file, Registered: false}
			migrations = append(migrations, migration)
		}
	}

	migrations = addInfoFromDBToMigrations(db, sortAndConnectMigrations(migrations), forUp)

	return migrations, nil
}

func sortAndConnectMigrations(migrations Migrations) Migrations {
	sort.Sort(migrations)

	// now that we're sorted in the appropriate direction,
	// populate next and previous for each migration
	for i, m := range migrations {
		prev := int64(-1)
		if i > 0 {
			prev = migrations[i-1].Version
			migrations[i-1].Next = m.Version
		}
		migrations[i].Previous = prev
	}

	return migrations
}

func convertDBInfoToMigration(dm *MigrationRecord) *Migration {
	var filename string

	if dm.FileName == nil {
		filename = "<nil>"
	} else {
		filename = fmt.Sprintf("[!DB info only!]:%s", *dm.FileName)
	}

	return &Migration{
		TStamp:    dm.TStamp,
		Version:   dm.VersionID,
		Next:      -1,
		Previous:  -1,
		Source:    filename,
		Note:      dm.Note,
		SQLData:   dm.SQLData,
		IsApplied: dm.IsApplied,
	}
}

func addInfoFromDBToMigrations(db *sql.DB, migrations Migrations, forUp bool) Migrations {
	rows, err := db.Query(fmt.Sprintf("SELECT tstamp, version_id, note, filename, is_applied, sqldata FROM %s ORDER by version_id, id;", TableName()))
	if err != nil {
		log.Fatal("error selecting from DB:", err)
	}
	defer rows.Close()

	var (
		row         MigrationRecord
		searchIndex int64
	)
	for rows.Next() {
		if err = rows.Scan(&row.TStamp, &row.VersionID, &row.Note, &row.FileName, &row.IsApplied, &row.SQLData); err != nil {
			log.Fatal("error scanning rows:", err)
		}

		if len(migrations) > 0 {
			for searchIndex < int64(len(migrations)-1) && row.VersionID > migrations[searchIndex].Version {
				searchIndex++
			}

			if row.VersionID == migrations[searchIndex].Version {
				migrations[searchIndex].IsApplied = row.IsApplied
				migrations[searchIndex].Note = row.Note
				migrations[searchIndex].TStamp = row.TStamp
			} else {
				if !forUp {
					migrations = append(migrations, convertDBInfoToMigration(&row))
				}
			}
		} else {
			if !forUp {
				migrations = append(migrations, convertDBInfoToMigration(&row))
			}
		}
	}
	return migrations
}

func versionFilter(v, current, target int64) bool {

	if target > current {
		return v > current && v <= target
	}

	if target < current {
		return v <= current && v > target
	}

	return false
}

// EnsureDBVersion retrieves the current version for this DB.
// Create and initialize the DB version table if it doesn't exist.
func EnsureDBVersion(db *sql.DB) (int64, error) {
	rows, err := GetDialect().dbVersionQuery(db)
	if err != nil {
		return 0, createVersionTable(db)
	}
	defer rows.Close()

	// The most recent record for each migration specifies
	// whether it has been applied or rolled back.
	// The first version we find that has been applied is the current version.

	toSkip := make([]int64, 0)

	for rows.Next() {
		var row MigrationRecord
		if err = rows.Scan(&row.VersionID, &row.FileName, &row.Note, &row.IsApplied); err != nil {
			log.Fatal("error scanning rows:", err)
		}

		// have we already marked this version to be skipped?
		skip := false
		for _, v := range toSkip {
			if v == row.VersionID {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		// if version has been applied we're done
		if row.IsApplied {
			return row.VersionID, nil
		}

		// latest version of migration has not been applied.
		toSkip = append(toSkip, row.VersionID)
	}

	return 0, ErrNoNextVersion
}

// Create the db version table
// and insert the initial 0 value into it
func createVersionTable(db *sql.DB) error {
	txn, err := db.Begin()
	if err != nil {
		return err
	}

	d := GetDialect()

	if _, err := txn.Exec(d.createVersionTableSQL()); err != nil {
		txn.Rollback()
		return err
	}

	version := 0
	note := "Init"
	applied := true
	if _, err := txn.Exec(d.insertVersionSQL(), version, nil, note, applied, nil); err != nil {
		txn.Rollback()
		return err
	}

	return txn.Commit()
}

// GetDBVersion is an alias for EnsureDBVersion, but returns -1 in error.
func GetDBVersion(db *sql.DB) (int64, error) {
	version, err := EnsureDBVersion(db)
	if err != nil {
		return -1, err
	}

	return version, nil
}
