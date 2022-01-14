package pgds

import (
	"context"
	"fmt"

	ds "github.com/ipfs/go-datastore"
	"github.com/jackc/pgx/v4"
)

type batch struct {
	ds    *Datastore
	batch *pgx.Batch
}

// Batch creates a set of deferred updates to the database.
func (d *Datastore) Batch() (ds.Batch, error) {
	b := &batch{ds: d, batch: &pgx.Batch{}}
	b.batch.Queue("BEGIN")
	return b, nil
}

func (b *batch) commitIfBatchFull() error {
	var err error

	if b.ds.maxBatchSize != 0 && b.batch.Len() >= int(b.ds.maxBatchSize) {
		err = b.CommitContext(context.Background())
	}

	if err != nil {
		b.batch = &pgx.Batch{}
		b.batch.Queue("BEGIN")
	}

	return err
}

func (b *batch) Put(key ds.Key, value []byte) error {
	sql := fmt.Sprintf("INSERT INTO %s (key, data) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET data = $2", b.ds.table)
	b.batch.Queue(sql, key.String(), value)
	return b.commitIfBatchFull()
}

func (b *batch) Delete(key ds.Key) error {
	b.batch.Queue(fmt.Sprintf("DELETE FROM %s WHERE key = $1", b.ds.table), key.String())
	return b.commitIfBatchFull()
}

func (b *batch) Commit() error {
	return b.CommitContext(context.Background())
}

func (b *batch) CommitContext(ctx context.Context) error {
	b.batch.Queue("COMMIT")
	res := b.ds.pool.SendBatch(ctx, b.batch)
	defer res.Close()

	for i := 0; i < b.batch.Len(); i++ {
		_, err := res.Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

var _ ds.Batching = (*Datastore)(nil)
