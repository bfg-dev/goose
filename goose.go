package goose

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"
)

var (
	duplicateCheckOnce sync.Once
	minVersion         = int64(0)
	maxVersion         = int64((1 << 63) - 1)
)

// Run runs a goose command.
func Run(command string, db *sql.DB, dir, note string, forceHoles bool, args ...string) error {
	switch command {
	case "up":
		if err := Up(db, dir, note, forceHoles); err != nil {
			return err
		}
	case "up-by-one":
		if err := UpByOne(db, dir, note); err != nil {
			return err
		}
	case "up-to":
		if len(args) == 0 {
			return fmt.Errorf("up-to must be of form: goose [OPTIONS] DRIVER DBSTRING up-to VERSION")
		}

		version, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("version must be a number (got '%s')", args[0])
		}
		if err := UpTo(db, dir, note, version, forceHoles); err != nil {
			return err
		}
	case "create":
		if len(args) == 0 {
			return fmt.Errorf("create must be of form: goose [OPTIONS] DRIVER DBSTRING create NAME [go|sql]")
		}

		migrationType := "go"
		if len(args) == 2 {
			migrationType = args[1]
		}
		if err := Create(db, dir, args[0], migrationType); err != nil {
			return err
		}
	case "down":
		if err := Down(db, dir, note); err != nil {
			return err
		}
	case "down-to":
		if len(args) == 0 {
			return fmt.Errorf("down-to must be of form: goose [OPTIONS] DRIVER DBSTRING down-to VERSION")
		}

		version, err := strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return fmt.Errorf("version must be a number (got '%s')", args[0])
		}
		if err := DownTo(db, dir, note, version); err != nil {
			return err
		}
	case "redo":
		if err := Redo(db, dir, note); err != nil {
			return err
		}
	case "reset":
		if err := Reset(db, dir, note); err != nil {
			return err
		}
	case "status":
		if err := Status(db, dir); err != nil {
			return err
		}
	case "version":
		if err := Version(db, dir); err != nil {
			return err
		}
	case "upgrade":
		var err, err2 error
		err = Upgrade(db, dir, note)
		err2 = Upgrade2(db, dir, note)
		if err == nil && err2 == nil {
			return nil
		}
		if err != nil && err2 == nil {
			return err
		}
		if err == nil && err2 != nil {
			return err2
		}
		return fmt.Errorf("Errors: %v and %v", err, err2)
	default:
		return fmt.Errorf("%q: no such command", command)
	}
	return nil
}
