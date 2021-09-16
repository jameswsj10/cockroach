// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestConcurrentProcessorsReadEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLEvalContext: &tree.EvalContextTestingKnobs{
				CallbackGenerators: map[string]*tree.CallbackValueGenerator{
					"my_callback": tree.NewCallbackValueGenerator(
						func(ctx context.Context, prev int, _ *kv.Txn) (int, error) {
							if prev < 10 {
								return prev + 1, nil
							}
							return -1, nil
						}),
				},
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	rows, err := db.Query(` select * from crdb_internal.testing_callback('my_callback')`)
	require.NoError(t, err)
	exp := 1
	for rows.Next() {
		var got int
		require.NoError(t, rows.Scan(&got))
		require.Equal(t, exp, got)
		exp++
	}
}

// TestSerialNormalizationWithUniqueUnorderedID makes sure that serial
// normalization uses unique_unordered_id() and a split in a table followed by
// insertions guarantees a (somewhat) uniform distribution of the data.
func TestSerialNormalizationWithUniqueUnorderedID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	params := base.TestServerArgs{}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// create a new table with serial primary key i (unordered_rowid) and int j (index)
	if _, err := db.Exec(`
SET serial_normalization TO 'unordered_rowid';
CREATE DATABASE t;
USE t;
CREATE TABLE t (
  i SERIAL PRIMARY KEY,
  j INT
);
`); err != nil {
		t.Fatal(err)
	}

	// enforce range splits to collect range statistics after row insertions
	_, err := db.Exec(`
ALTER TABLE t SPLIT AT SELECT i<<(60) FROM generate_series(1, 7) as t(i); -- 3 bits worth of splits in the high order
`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
INSERT INTO t(j) SELECT * FROM generate_series(1, 1000000);
`)
	if err != nil {
		t.Fatal(err)
	}

	// Derive range statistics
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	runner := sqlutils.MakeSQLRunner(conn)
	var queryString [][]string = runner.QueryStr(t, "SELECT start_pretty,"+
		"end_pretty, crdb_internal.range_stats(start_key)->'key_count' AS rows FROM "+
		"crdb_internal.ranges_no_leases WHERE table_id ='t'::regclass;")

	// Output query
	fmt.Println("Table t with range statistics")
	for _, row := range queryString {
		fmt.Println(row)
	}
}
