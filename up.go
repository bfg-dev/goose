package goose

import (
	"database/sql"
)

// UpTo migrates up to a specific version.
func UpTo(db *sql.DB, dir, note string, version int64, forceHoles bool) error {
	var next *Migration

	// Just create table if it does not exist
	_, err := GetDBVersion(db)

	migrations, err := CollectMigrations(db, dir, minVersion, version, true)
	if err != nil {
		return err
	}

	for {
		current, err := GetDBVersion(db)
		if err != nil {
			return err
		}

		if forceHoles {
			next, err = migrations.FirstPending()
			if err != nil {
				if err == ErrNoPendingVersion {
					log.Printf("goose: no migrations to run. current version: %d\n", current)
					return nil
				}
				return err
			}
		} else {
			next, err = migrations.Next(current)
			if err != nil {
				if err == ErrNoNextVersion {
					log.Printf("goose: no migrations to run. current version: %d\n", current)
					return nil
				}
				return err
			}
		}

		//log.Print(next.Source)

		if err = next.Up(db, note); err != nil {
			return err
		}
	}
}

// Up applies all available migrations.
func Up(db *sql.DB, dir, note string, forceHoles bool) error {
	return UpTo(db, dir, note, maxVersion, forceHoles)
}

// UpByOne migrates up by a single version.
func UpByOne(db *sql.DB, dir, note string) error {
	currentVersion, err := GetDBVersion(db)
	if err != nil {
		return err
	}

	migrations, err := CollectMigrations(db, dir, minVersion, maxVersion, true)
	if err != nil {
		return err
	}

	next, err := migrations.Next(currentVersion)
	if err != nil {
		if err == ErrNoNextVersion {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
		}
		return err
	}

	if err = next.Up(db, note); err != nil {
		return err
	}

	return nil
}
