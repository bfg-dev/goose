package goose

import (
	"database/sql"
	"fmt"
	"path/filepath"
)

// Upgrade migraion table
func Upgrade(db *sql.DB, dir, note string) error {
	var (
		row         MigrationRecord
		searchIndex int64
	)

	rows, err := GetDialect().dbVersionQuery(db)
	if err != nil {
		if err := createVersionTable(db); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("table %s already exists", TableName())
	}
	rows.Close()

	// collect all migrations
	migrations, err := CollectMigrations(db, dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	rows, err = db.Query(fmt.Sprintf("SELECT version_id, is_applied, tstamp FROM %s;", TableOldName()))
	if err != nil {
		return err
	}

	for rows.Next() {
		err := rows.Scan(&row.VersionID, &row.IsApplied, &row.TStamp)
		if err != nil {
			return err
		}

		for row.VersionID > migrations[searchIndex].Version && searchIndex < int64(len(migrations)-1) {
			searchIndex++
		}

		if row.VersionID == migrations[searchIndex].Version {
			m := migrations[searchIndex]
			if _, err := db.Exec(GetDialect().insertVersionSQL(), row.VersionID, filepath.Base(m.Source), note, row.IsApplied); err != nil {
				return err
			}
		}

	}

	return nil
}
