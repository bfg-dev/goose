package goose

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"path/filepath"
)

func checkUpgrade(db *sql.DB) error {
	drows, err := GetDialect().dbVersionQuery(db)
	if err != nil {
		if err := createVersionTable(db); err != nil {
			return err
		}
		return nil
	}

	drows.Close()
	return fmt.Errorf("table %s already exists", TableName())
}

// Upgrade migraion table
func Upgrade(db *sql.DB, dir, note string) error {
	var (
		row         MigrationRecord
		searchIndex int64
	)

	if err := checkUpgrade(db); err != nil {
		return err
	}

	// collect all migrations
	migrations, err := CollectMigrations(db, dir, minVersion, maxVersion, false)
	if err != nil {
		return err
	}

	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied, tstamp FROM %s;", TableOldName()))
	if err != nil {
		return err
	}
	defer rows.Close()

	tx, err := db.Begin()
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
			sqldata, err := ioutil.ReadFile(migrations[searchIndex].Source)
			if err != nil {
				tx.Rollback()
				return err
			}
			if _, err := tx.Exec(GetDialect().insertVersionSQL(), row.VersionID, filepath.Base(migrations[searchIndex].Source), note, row.IsApplied, string(sqldata)); err != nil {
				tx.Rollback()
				return err
			}
		}
	}

	return tx.Commit()
}

// Upgrade2 migraion table
func Upgrade2(db *sql.DB, dir, note string) error {
	upgradetx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = upgradetx.Exec(fmt.Sprintf("ALTER TABLE \"%s\" ADD COLUMN sqldata TEXT DEFAULT NULL;", tableName))
	if err != nil {
		upgradetx.Rollback()
	} else {
		upgradetx.Commit()
	}

	// collect all migrations
	migrations, err := CollectMigrations(db, dir, minVersion, maxVersion, false)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, migration := range migrations {
		if migration.Source == "<nil>" {
			continue
		}
		sqldata, err := ioutil.ReadFile(migration.Source)
		if err != nil {
			tx.Rollback()
			return err
		}

		if _, err := tx.Exec(fmt.Sprintf("UPDATE %s SET sqldata = $1 WHERE version_id = $2;", tableName), string(sqldata), migration.Version); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}
