//go:build duckdb

package duckstore

import (
	"database/sql"
	"os"
	"testing"
)

func TestZZDumpAST(t *testing.T) {
	db, _ := sql.Open("duckdb", "")
	q := `SELECT CAST(((tag2::BIGINT&4294967295)::HUGEINT*4294967296+(tag1::BIGINT&4294967295)+9223372036854775808)%18446744073709551616-9223372036854775808 AS BIGINT) AS _tag1, (time+270000)//60*60-270000 AS _time, toFloat64(sum(count)) AS _val0, sh_merge_percentiles(list(percentiles)) AS _val1, argMaxMergeState(max_host) AS _maxHost, stag2, _shard_num, CAST(epoch(timezone('Europe/Moscow',date_trunc('month',timezone('Europe/Moscow',to_timestamp(time))))) AS BIGINT) AS m FROM statshouse_v6_1s_dist WHERE time>=1 AND time<2 AND index_type=0 AND pre_tag=0 AND pre_stag='' AND metric IN (1,2) AND (tag1 IN (3) OR NOT match(stag2,'a''b') OR NOT (tag0=0 AND stag0='')) GROUP BY _time,_tag1,stag2,_shard_num HAVING _val0>0 ORDER BY _time,_tag1 DESC LIMIT 10`
	var js string
	if err := db.QueryRow("SELECT CAST(json_serialize_sql(?::VARCHAR) AS VARCHAR)", q).Scan(&js); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(os.Getenv("OUT")+"/ast.json", []byte(js), 0o644)
	db.QueryRow("SELECT CAST(json_serialize_sql(?::VARCHAR) AS VARCHAR)", `SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT 1')), (SELECT 1) s WHERE 1 IN (SELECT 2)`).Scan(&js)
	os.WriteFile(os.Getenv("OUT")+"/ast2.json", []byte(js), 0o644)
}
