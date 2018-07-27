package goose

import (
	"database/sql"
)

// Version prints the current version of the database.
func Version(db *sql.DB, dir string) error {
	current, err := GetDBVersion(db)
	if err != nil {
		return err
	}

	log.Printf("goose: version %v\n", current)
	return nil
}

var (
	tableName    = "goose_db_version_v2"
	tableOldName = "goose_db_version"
)

// TableName returns goose db version table name
func TableName() string {
	return tableName
}

// SetTableName set goose db version table name
func SetTableName(n string) {
	tableName = n
}

// TableName returns goose db version table name
func TableOldName() string {
	return tableOldName
}

// SetTableName set goose db version table name
func SetTableOldName(n string) {
	tableOldName = n
}
