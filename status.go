package goose

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"time"
)

// Status prints the status of all migrations.
func Status(db *sql.DB, dir string) error {
	// must ensure that the version table exists if we're running on a pristine DB
	if _, err := EnsureDBVersion(db); err != nil {
		return err
	}

	// collect all migrations
	migrations, err := CollectMigrations(db, dir, minVersion, maxVersion)
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
	var row MigrationRecord
	q := fmt.Sprintf("SELECT tstamp, filename, note, is_applied FROM %s WHERE version_id=%d ORDER BY tstamp DESC LIMIT 1", TableName(), migration.Version)
	e := db.QueryRow(q).Scan(&row.TStamp, &row.FileName, &row.Note, &row.IsApplied)

	if e != nil && e != sql.ErrNoRows {
		log.Fatal(e)
	}

	var (
		appliedAt string
		filename  string
		note      string
	)

	if row.IsApplied {
		appliedAt = row.TStamp.Format(time.ANSIC)
	} else {
		appliedAt = "Pending"
	}

	if row.FileName != nil {
		filename = *row.FileName
	} else {
		filename = filepath.Base(migration.Source)
	}

	if row.Note != nil && len(*row.Note) > 0 {
		note = *row.Note
	} else {
		note = "-"
	}

	log.Printf("    %-24s -- %v  (%v)\n", appliedAt, filename, note)
}
