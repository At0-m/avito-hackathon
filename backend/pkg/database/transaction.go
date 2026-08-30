package database

import (
	"context"
	"database/sql"
	"fmt"
)

func (p *PostgresDB) WithinTransaction(
	ctx context.Context,
	opts *sql.TxOptions,
	fn func(*sql.Tx) error,
) (err error) {
	tx, err := p.DB.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.Rollback()
			panic(recovered)
		}
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
