package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

var transactionDriverCounter atomic.Int64

type transactionDriver struct {
	state *transactionState
}

type transactionState struct {
	mu        sync.Mutex
	begins    int
	commits   int
	rollbacks int
	options   driver.TxOptions
}

func (d transactionDriver) Open(string) (driver.Conn, error) {
	return &transactionConn{state: d.state}, nil
}

type transactionConn struct {
	state *transactionState
}

func (c *transactionConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}
func (c *transactionConn) Close() error { return nil }
func (c *transactionConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}
func (c *transactionConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	c.state.mu.Lock()
	c.state.begins++
	c.state.options = options
	c.state.mu.Unlock()
	return &transactionTx{state: c.state}, nil
}

type transactionTx struct {
	state *transactionState
}

func (t *transactionTx) Commit() error {
	t.state.mu.Lock()
	t.state.commits++
	t.state.mu.Unlock()
	return nil
}
func (t *transactionTx) Rollback() error {
	t.state.mu.Lock()
	t.state.rollbacks++
	t.state.mu.Unlock()
	return nil
}

func newTransactionTestDB(t *testing.T) (*PostgresDB, *transactionState) {
	t.Helper()
	state := &transactionState{}
	name := fmt.Sprintf("transaction-test-%d", transactionDriverCounter.Add(1))
	sql.Register(name, transactionDriver{state: state})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &PostgresDB{DB: db}, state
}

func TestWithinTransactionCommitsSuccess(t *testing.T) {
	db, state := newTransactionTestDB(t)
	called := false

	err := db.WithinTransaction(context.Background(), nil, func(*sql.Tx) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("within transaction: %v", err)
	}
	if !called {
		t.Fatal("callback was not called")
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.begins != 1 || state.commits != 1 || state.rollbacks != 0 {
		t.Fatalf("unexpected lifecycle: begin=%d commit=%d rollback=%d", state.begins, state.commits, state.rollbacks)
	}
}

func TestWithinTransactionRollsBackError(t *testing.T) {
	db, state := newTransactionTestDB(t)
	want := errors.New("write failed")

	err := db.WithinTransaction(context.Background(), nil, func(*sql.Tx) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.commits != 0 || state.rollbacks != 1 {
		t.Fatalf("unexpected lifecycle: commit=%d rollback=%d", state.commits, state.rollbacks)
	}
}

func TestWithinTransactionRollsBackPanic(t *testing.T) {
	db, state := newTransactionTestDB(t)

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		_ = db.WithinTransaction(context.Background(), nil, func(*sql.Tx) error {
			panic("boom")
		})
	}()

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.commits != 0 || state.rollbacks != 1 {
		t.Fatalf("unexpected lifecycle: commit=%d rollback=%d", state.commits, state.rollbacks)
	}
}

func TestWithinTransactionPassesOptions(t *testing.T) {
	db, state := newTransactionTestDB(t)
	opts := &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: true}

	if err := db.WithinTransaction(context.Background(), opts, func(*sql.Tx) error { return nil }); err != nil {
		t.Fatalf("within transaction: %v", err)
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.options.Isolation != driver.IsolationLevel(sql.LevelSerializable) || !state.options.ReadOnly {
		t.Fatalf("unexpected options: %+v", state.options)
	}
}
