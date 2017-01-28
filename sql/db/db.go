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

func New(engine, source string, opts ...Option) (*Conn, error) {
	var d dbr.Dialect
	switch engine {
	case MYSQL:
		d = dialect.MySQL

	case POSTGRESQL:
		d = dialect.PostgreSQL

	case SQLITE3:
		d = dialect.SQLite3

	default:
		return nil, fmt.Errorf("unsupport engine %q", engine)
	}

	db, err := sql.Open(engine, source)
	if err != nil {
		return nil, err
	}

	conn := &Conn{
		DB: db,
		SQL: Builder{
			d: d,
		},
	}

	for _, one := range opts {
		if one != nil {
			one(conn)
		}
	}

	return conn, nil
}

func Open(engine, source string, opts ...Option) error {
	if engine == "" {
		engine = defaultEngine
	}

	mu.Lock()
	defer mu.Unlock()

	if _, ok := instances[engine]; ok {
		return fmt.Errorf("duplicate db instance %q", engine)
	}

	conn, err := New(engine, source, opts...)
	if err != nil {
		return err
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
	SQL Builder
}
