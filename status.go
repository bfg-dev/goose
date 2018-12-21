package goose

import (
	"database/sql"
	"path/filepath"
	"strings"
	"time"
)

// Status prints the status of all migrations.
func Status(db *sql.DB, dir string) error {
	// must ensure that the version table exists if we're running on a pristine DB
	if _, err := EnsureDBVersion(db); err != nil {
		return err
	}

	// collect all migrations
	migrations, err := CollectMigrations(db, dir, minVersion, maxVersion, false)
	if err != nil {
		return err
	}

	log.Println("    Applied At                  Migration                  (Note)")
	log.Println("    =============================================================")
	for _, migration := range migrations {
		printMigrationStatus(db, migration)
	}

	return nil
}

func printMigrationStatus(db *sql.DB, migration *Migration) {
	var (
		appliedAt string
		note      string
		filename  string
	)

	if migration.IsApplied {
		appliedAt = migration.TStamp.Format(time.ANSIC)
	} else {
		appliedAt = "Pending"
	}

	if migration.Note != nil && len(*migration.Note) > 0 {
		note = *migration.Note
	} else {
		note = "-"
	}

	if strings.HasPrefix(migration.Source, "[!") {
		filename = migration.Source
	} else {
		filename = filepath.Base(migration.Source)
	}

	log.Printf("    %-24s -- %v  (%v)\n", appliedAt, filename, note)
}
