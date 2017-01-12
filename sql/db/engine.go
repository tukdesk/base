package db

const (
	MYSQL      = "mysql"
	POSTGRESQL = "postgres"
	SQLITE3    = "sqlite3"
)

var defaultEngine string

func DefaultEngine(engine string) {
	defaultEngine = engine
}
