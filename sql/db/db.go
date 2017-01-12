package db

import (
	"database/sql"
	"fmt"
	"sync"

	"github.com/gocraft/dbr"
	"github.com/gocraft/dbr/dialect"
)

var (
	mu              sync.RWMutex
	instances       = map[string]*Conn{}
	nullEvtReceiver = &dbr.NullEventReceiver{}
)

func Open(engine, source string, opts ...Option) error {
	if engine == "" {
		engine = defaultEngine
	}

	mu.Lock()
	defer mu.Unlock()

	if _, ok := instances[engine]; ok {
		return fmt.Errorf("duplicate db instance %q", engine)
	}

	var d dbr.Dialect
	switch engine {
	case MYSQL:
		d = dialect.MySQL

	case POSTGRESQL:
		d = dialect.PostgreSQL

	case SQLITE3:
		d = dialect.SQLite3

	default:
		return fmt.Errorf("unsupport engine %q", engine)
	}

	db, err := sql.Open(engine, source)
	if err != nil {
		return err
	}

	conn := &Conn{
		DB: db,
		d:  d,
	}

	for _, one := range opts {
		if one != nil {
			one(conn)
		}
	}

	instances[engine] = conn

	return nil
}

func Get(engine string) (*Conn, error) {
	if engine == "" {
		engine = defaultEngine
	}

	mu.RLock()
	instace := instances[engine]
	mu.RUnlock()

	if instace == nil {
		return nil, fmt.Errorf("db instance for %q not found", engine)
	}

	return instace, nil
}

func MustGet(engine string) *Conn {
	instance, err := Get(engine)
	if err != nil {
		panic(err)
	}

	return instance
}

type Conn struct {
	*sql.DB
	d dbr.Dialect
}

func (this *Conn) BuildSQL(stmt dbr.Builder) (string, error) {
	return dbr.InterpolateForDialect("?", []interface{}{stmt}, this.d)
}

func (this *Conn) MustBuildSQL(stmt dbr.Builder) string {
	query, err := this.BuildSQL(stmt)
	if err != nil {
		panic(err)
	}

	return query
}

func (Conn) InsertInto(table string) *dbr.InsertStmt {
	return dbr.InsertInto(table)
}

func (Conn) InsertBySQL(query string, value ...interface{}) *dbr.InsertStmt {
	return dbr.InsertBySql(query, value...)
}

func (this *Conn) Select(colunm ...string) *dbr.SelectStmt {
	c := make([]interface{}, len(colunm))
	for i, one := range colunm {
		c[i] = one
	}

	return this.SelectEx(c...)
}

func (Conn) SelectEx(colunm ...interface{}) *dbr.SelectStmt {
	return dbr.Select(colunm...)
}

func (Conn) SelectBySQL(query string, value ...interface{}) *dbr.SelectStmt {
	return dbr.SelectBySql(query, value...)
}

func (Conn) Update(table string) *dbr.UpdateStmt {
	return dbr.Update(table)
}

func (Conn) UpdateBySQL(query string, value ...interface{}) *dbr.UpdateStmt {
	return dbr.UpdateBySql(query, value...)
}

func (Conn) DeleteFrom(table string) *dbr.DeleteStmt {
	return dbr.DeleteFrom(table)
}

func (Conn) DeleteBySQL(query string, value ...interface{}) *dbr.DeleteStmt {
	return dbr.DeleteBySql(query, value...)
}

func (Conn) Expr(query string, value ...interface{}) dbr.Builder {
	return dbr.Expr(query, value...)
}

func (Conn) And(cond ...dbr.Builder) dbr.Builder {
	return dbr.And(cond...)
}

func (Conn) Or(cond ...dbr.Builder) dbr.Builder {
	return dbr.Or(cond...)
}

func (Conn) Eq(column string, value interface{}) dbr.Builder {
	return dbr.Eq(column, value)
}

func (Conn) Neq(column string, value interface{}) dbr.Builder {
	return dbr.Neq(column, value)
}

func (Conn) Gt(column string, value interface{}) dbr.Builder {
	return dbr.Gt(column, value)
}

func (Conn) Gte(column string, value interface{}) dbr.Builder {
	return dbr.Gte(column, value)
}

func (Conn) Lt(column string, value interface{}) dbr.Builder {
	return dbr.Lt(column, value)
}

func (Conn) Lte(column string, value interface{}) dbr.Builder {
	return dbr.Lte(column, value)
}
