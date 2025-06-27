package main

import (
	"context"
	"database/sql"
	"log"
	"time"
)

const MariaDb = "mariadb-11.8.2"
const MySql8 = "mysql-8.4.5"
const MySql9 = "mysql-9.3.0"
const PostgreSql16 = "pg-16.9"
const PostgreSql17 = "pg-17.5"
const PostgreSql18 = "pg-18-beta1"
const MsSql19 = "mssql-19-CU32"
const MsSql22 = "mssql-22-CU19"
const MsSql25 = "mssql-25-CTP2.0"

type testData struct {
	testName  string
	queries   map[string]map[string]string
	f         func(context.Context, *sql.DB, string)
	execCount int
}

var Tests = map[string]testData{
	// access
	"index-seek-vs-scan": {
		"nonclustered index seek vs. scan",
		map[string]map[string]string{
			//MySql8: {
			//	"a - 1 row":     "select min(name) from client where country = 'UK';",
			//	"b - 9 rows":    "select min(name) from client where country = 'NL';",
			//	"c - 90 rows":   "select min(name) from client where country = 'FR';",
			//	"d - 900 rows":  "select min(name) from client where country = 'CY';",
			//	"e - 4000 rows": "select min(name) from client where country = 'US';",
			//	"f - 7333 rows": "select min(name) from client where country >= 'US';",
			//},
			MySql9: {
				"a - 1 row":     "select min(name) from client where country = 'UK';",
				"b - 9 rows":    "select min(name) from client where country = 'NL';",
				"c - 90 rows":   "select min(name) from client where country = 'FR';",
				"d - 900 rows":  "select min(name) from client where country = 'CY';",
				"e - 4000 rows": "select min(name) from client where country = 'US';",
				"f - 7333 rows": "select min(name) from client where country >= 'US';",
			},
			PostgreSql17: {
				"a - 1 row":     "select min(name) from client where country = 'UK';",
				"b - 9 rows":    "select min(name) from client where country = 'NL';",
				"c - 90 rows":   "select min(name) from client where country = 'FR';",
				"d - 900 rows":  "select min(name) from client where country = 'CY';",
				"e - 4000 rows": "select min(name) from client where country = 'US';",
				"f - 7333 rows": "select min(name) from client where country >= 'US';",
			},
			//PostgreSql18: {
			//	"a - 1 row":     "select min(name) from client where country = 'UK';",
			//	"b - 9 rows":    "select min(name) from client where country = 'NL';",
			//	"c - 90 rows":   "select min(name) from client where country = 'FR';",
			//	"d - 900 rows":  "select min(name) from client where country = 'CY';",
			//	"e - 4000 rows": "select min(name) from client where country = 'US';",
			//	"f - 7333 rows": "select min(name) from client where country >= 'US';",
			//},
			MsSql22: {
				"a - 1 row":                 "select min(name) from client where country = 'UK';",
				"b - 9 rows":                "select min(name) from client where country = 'NL';",
				"c - 90 rows":               "select min(name) from client where country = 'FR';",
				"c - 90 rows (forceseek)":   "select min(name) from client with (forceseek) where country = 'FR';",
				"d - 900 rows":              "select min(name) from client where country = 'CY';",
				"d - 900 rows (forceseek)":  "select min(name) from client with (forceseek) where country = 'CY';",
				"e - 4000 rows":             "select min(name) from client where country = 'US';",
				"e - 4000 rows (forceseek)": "select min(name) from client with (forceseek) where country = 'US';",
				"f - 7333 rows":             "select min(name) from client where country >= 'US';",
			},
			//MsSql25: {
			//	"a - 1 row":                 "select min(name) from client where country = 'UK';",
			//	"b - 9 rows":                "select min(name) from client where country = 'NL';",
			//	"c - 90 rows":               "select min(name) from client where country = 'FR';",
			//	"c - 90 rows (forceseek)":   "select min(name) from client with (forceseek) where country = 'FR';",
			//	"d - 900 rows":              "select min(name) from client where country = 'CY';",
			//	"d - 900 rows (forceseek)":  "select min(name) from client with (forceseek) where country = 'CY';",
			//	"e - 4000 rows":             "select min(name) from client where country = 'US';",
			//	"e - 4000 rows (forceseek)": "select min(name) from client with (forceseek) where country = 'US';",
			//	"f - 7333 rows":             "select min(name) from client where country >= 'US';",
			//},
		},
		QueryString,
		200,
	},
	"index-seek-vs-scan-large": {
		"nonclustered index seek vs. scan",
		map[string]map[string]string{
			MySql9: {
				"a - 100 row":      "select min(name) from client_large where country = 'UK';",
				"b - 900 rows":     "select min(name) from client_large where country = 'NL';",
				"c - 9,000 rows":   "select min(name) from client_large where country = 'FR';",
				"d - 90,000 rows":  "select min(name) from client_large where country = 'CY';",
				"e - 400,000 rows": "select min(name) from client_large where country = 'US';",
				"f - 733,333 rows": "select min(name) from client_large where country >= 'US';",
			},
			PostgreSql17: {
				"a - 100 row":      "select min(name) from client_large where country = 'UK';",
				"b - 900 rows":     "select min(name) from client_large where country = 'NL';",
				"c - 9,000 rows":   "select min(name) from client_large where country = 'FR';",
				"d - 90,000 rows":  "select min(name) from client_large where country = 'CY';",
				"e - 400,000 rows": "select min(name) from client_large where country = 'US';",
				"f - 733,333 rows": "select min(name) from client_large where country >= 'US';",
			},
			MsSql22: {
				"a - 100 row":                  "select min(name) from client_large where country = 'UK';",
				"b - 900 rows":                 "select min(name) from client_large where country = 'NL';",
				"c - 9,000 rows":               "select min(name) from client_large where country = 'FR';",
				"c - 9,000 rows (forceseek)":   "select min(name) from client_large with (forceseek) where country = 'FR';",
				"d - 90,000 rows":              "select min(name) from client_large where country = 'CY';",
				"d - 90,000 rows (forceseek)":  "select min(name) from client_large with (forceseek) where country = 'CY';",
				"e - 400,000 rows":             "select min(name) from client_large where country = 'US';",
				"e - 400,000 rows (forceseek)": "select min(name) from client_large with (forceseek) where country = 'US';",
				"f - 733,333 rows":             "select min(name) from client_large where country >= 'US';",
			},
		},
		QueryString,
		5,
	},
	"clustered-index-seek-id": {
		"clustered index seek",
		map[string]map[string]string{
			MySql9: {
				"a - small": "select id from client where id = 5000;",
				"b - large": "select id from client_large where id = 500000;",
			},
			PostgreSql17: {
				"a - small": "select id from client where id = 5000;",
				"b - large": "select id from client_large where id = 500000;",
			},
			MsSql22: {
				"a - small": "select id from client where id = 5000;",
				"b - large": "select id from client_large where id = 500000;",
			},
		},
		QueryInt,
		500,
	},
	"clustered-index-seek-name": {
		"clustered index seek",
		map[string]map[string]string{
			MySql9: {
				"a - small": "select name from client where id = 5000;",
				"b - large": "select name from client_large where id = 500000;",
			},
			PostgreSql17: {
				"a - small": "select name from client where id = 5000;",
				"b - large": "select name from client_large where id = 500000;",
			},
			MsSql22: {
				"a - small": "select name from client where id = 5000;",
				"b - large": "select name from client_large where id = 500000;",
			},
		},
		QueryString,
		500,
	},
	"clustered-index-range": {
		"clustered index range",
		map[string]map[string]string{
			MySql8: {
				"a - small":               "select min(name) from client where id >= 3000 and id < 5000;",
				"b - large":               "select min(name) from client_large where id >= 300000 and id < 500000",
				"c - large - small range": "select min(name) from client_large where id >= 300000 and id < 320000",
			},
			MySql9: {
				"a - small":               "select min(name) from client where id >= 3000 and id < 5000;",
				"b - large":               "select min(name) from client_large where id >= 300000 and id < 500000",
				"c - large - small range": "select min(name) from client_large where id >= 300000 and id < 320000",
			},
			PostgreSql17: {
				"a - small":               "select min(name) from client where id >= 3000 and id < 5000;",
				"b - large":               "select min(name) from client_large where id >= 300000 and id < 500000",
				"c - large - small range": "select min(name) from client_large where id >= 300000 and id < 320000",
			},
			PostgreSql18: {
				"a - small":               "select min(name) from client where id >= 3000 and id < 5000;",
				"b - large":               "select min(name) from client_large where id >= 300000 and id < 500000",
				"c - large - small range": "select min(name) from client_large where id >= 300000 and id < 320000",
			},
			MsSql22: {
				"a - small":               "select min(name) from client where id >= 3000 and id < 5000;",
				"b - large":               "select min(name) from client_large where id >= 300000 and id < 500000",
				"c - large - small range": "select min(name) from client_large where id >= 300000 and id < 320000",
			},
			MsSql25: {
				"a - small":               "select min(name) from client where id >= 3000 and id < 5000;",
				"b - large":               "select min(name) from client_large where id >= 300000 and id < 500000",
				"c - large - small range": "select min(name) from client_large where id >= 300000 and id < 320000",
			},
		},
		QueryString,
		30,
	},
	"table-scan": {
		"",
		map[string]map[string]string{
			MySql9: {
				"a - tinyint - 10%": "select count(*) from filter_1m where status_id_tinyint = 0;",
				"a - tinyint - 90%": "select count(*) from filter_1m where status_id_tinyint = 1;",
				"b - int - 10%":     "select count(*) from filter_1m where status_id_int = 0;",
				"b - int - 90%":     "select count(*) from filter_1m where status_id_int = 1;",
				"c - char - 10%":    "select count(*) from filter_1m where status_char = 'deleted';",
				"c - char - 90%":    "select count(*) from filter_1m where status_char = 'active';",
				"d - varchar - 10%": "select count(*) from filter_1m where status_varchar = 'deleted';",
				"d - varchar - 90%": "select count(*) from filter_1m where status_varchar = 'active';",
				"e - text - 10%":    "select count(*) from filter_1m where status_text = 'deleted';",
				"e - text - 90%":    "select count(*) from filter_1m where status_text = 'active';",
			},
			PostgreSql17: {
				"a - tinyint - 10%": "select count(*) from filter_1m where status_id_tinyint = 0;",
				"a - tinyint - 90%": "select count(*) from filter_1m where status_id_tinyint = 1;",
				"b - int - 10%":     "select count(*) from filter_1m where status_id_int = 0;",
				"b - int - 90%":     "select count(*) from filter_1m where status_id_int = 1;",
				"c - char - 10%":    "select count(*) from filter_1m where status_char = 'deleted';",
				"c - char - 90%":    "select count(*) from filter_1m where status_char = 'active';",
				"d - varchar - 10%": "select count(*) from filter_1m where status_varchar = 'deleted';",
				"d - varchar - 90%": "select count(*) from filter_1m where status_varchar = 'active';",
				"e - text - 10%":    "select count(*) from filter_1m where status_text = 'deleted';",
				"e - text - 90%":    "select count(*) from filter_1m where status_text = 'active';",
			},
			MsSql22: {
				"a - tinyint - 10%": "select count(*) from filter_1m where status_id_tinyint = 0;",
				"a - tinyint - 90%": "select count(*) from filter_1m where status_id_tinyint = 1;",
				"b - int - 10%":     "select count(*) from filter_1m where status_id_int = 0;",
				"b - int - 90%":     "select count(*) from filter_1m where status_id_int = 1;",
				"c - char - 10%":    "select count(*) from filter_1m where status_char = 'deleted';",
				"c - char - 90%":    "select count(*) from filter_1m where status_char = 'active';",
				"d - varchar - 10%": "select count(*) from filter_1m where status_varchar = 'deleted';",
				"d - varchar - 90%": "select count(*) from filter_1m where status_varchar = 'active';",
				"e - text - 10%":    "select count(*) from filter_1m where status_text = 'deleted';",
				"e - text - 90%":    "select count(*) from filter_1m where status_text = 'active';",
			},
		},
		QueryInt,
		10,
	},

	"dml": {
		"postgres index only scan behaviour",
		map[string]map[string]string{
			MySql9: {
				"index only scan":              "select min(ts), max(description) from (select ts, description from transactions where ts < '2020-01-01 01:00:00') as t;",
				"index only scan after update": "select min(ts), max(description) from (select ts, description from transactions_modified where ts < '2020-01-01 01:00:00') as t;",
				"index scan after update":      "select min(ts), max(description) from (select ts, description from transactions_wo_covered_index where ts < '2020-01-01 01:00:00') as t;",
			},
			PostgreSql17: {
				"index only scan":              "select min(ts), max(description) from (select ts, description from transactions where ts < '2020-01-01 01:00:00') as t;",
				"index only scan after update": "select min(ts), max(description) from (select ts, description from transactions_modified where ts < '2020-01-01 01:00:00') as t;",
				"index scan after update":      "select min(ts), max(description) from (select ts, description from transactions_wo_covered_index where ts < '2020-01-01 01:00:00') as t;",
			},
			MsSql22: {
				"index only scan":              "select min(ts), max(description) from (select ts, description from transactions where ts < '2020-01-01 01:00:00') as t;",
				"index only scan after update": "select min(ts), max(description) from (select ts, description from transactions_modified where ts < '2020-01-01 01:00:00') as t;",
				"index scan after update":      "select min(ts), max(description) from (select ts, description from transactions_wo_covered_index where ts < '2020-01-01 01:00:00') as t;",
			}},
		QueryTsAndString,
		100,
	},

	// skip scan
	"distinct-count": {
		"select distinct / count distinct",
		map[string]map[string]string{
			MariaDb: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			},
			MySql8: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			},
			MySql9: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			},
			PostgreSql16: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			},
			PostgreSql17: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			},
			PostgreSql18: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			},
			MsSql22: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			},
			MsSql25: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			}},
		QueryInt,
		20,
	},
	"distinct-count-ex": {
		"select distinct / count distinct",
		map[string]map[string]string{
			MySql9: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
			},
			PostgreSql17: {
				"a": "select count(distinct a) as cnt from group_by_table",
				"b": "select count(distinct b) as cnt from group_by_table",
				//"b-sequential": "set max_parallel_workers_per_gather = 1; select count(distinct b) as cnt from group_by_table",
				"c": "select count(distinct c) as cnt from group_by_table",
				//"c-sequential": "set max_parallel_workers_per_gather = 1; select count(distinct c) as cnt from group_by_table",
				"a-recursive": "with recursive t as (select min(a) as x from group_by_table union all select (select min(a) from group_by_table where a > t.x) from t where t.x is not null) select count(*) from (select x from t where x is not null union all select null where exists (select 1 from group_by_table where a is null)) as tmp;",
				"b-recursive": "with recursive t as (select min(b) as x from group_by_table union all select (select min(b) from group_by_table where b > t.x) from t where t.x is not null) select count(*) from (select x from t where x is not null union all select null where exists (select 1 from group_by_table where b is null)) as tmp;",
				"c-recursive": "with recursive t as (select min(c) as x from group_by_table union all select (select min(c) from group_by_table where c > t.x) from t where t.x is not null) select count(*) from (select x from t where x is not null union all select null where exists (select 1 from group_by_table where c is null)) as tmp;",
			},
			MsSql22: {
				"a":               "select count(distinct a) as cnt from group_by_table",
				"b":               "select count(distinct b) as cnt from group_by_table",
				"c":               "select count(distinct c) as cnt from group_by_table",
				"a-temp-table":    "create table #result (x int); declare @current int; select top (1) @current = a from group_by_table order by a; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = a from group_by_table where a > @current order by a; end; select count(*) from #result;",
				"b-temp-table":    "create table #result (x int); declare @current int; select top (1) @current = b from group_by_table order by b; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = b from group_by_table where b > @current order by b; end; select count(*) from #result;",
				"c-temp-table":    "create table #result (x int); declare @current int; select top (1) @current = c from group_by_table order by c; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = c from group_by_table where c > @current order by c; end; select count(*) from #result;",
				"a-numbers-table": "with min_max as (select min(a) as min_a, max(a) as max_a from group_by_table), possible_values as (select n.id from numbers as n inner join min_max as mm on n.id >= mm.min_a and n.id <= mm.max_a), result as (select pv.id from possible_values as pv where exists (select top (1) 1 from group_by_table as g where g.a = pv.id)) select count(*) from result;",
				"b-numbers-table": "with min_max as (select min(b) as min_b, max(b) as max_b from group_by_table), possible_values as (select n.id from numbers as n inner join min_max as mm on n.id >= mm.min_b and n.id <= mm.max_b), result as (select pv.id from possible_values as pv where exists (select top (1) 1 from group_by_table as g where g.b = pv.id)) select count(*) from result;",
				"c-numbers-table": "with min_max as (select min(c) as min_c, max(c) as max_c from group_by_table), possible_values as (select n.id from numbers as n inner join min_max as mm on n.id >= mm.min_c and n.id <= mm.max_c), result as (select pv.id from possible_values as pv where exists (select top (1) 1 from group_by_table as g where g.c = pv.id)) select count(*) from result;",
			},
		},
		QueryInt,
		20,
	},
	"skip-scan-1": {
		"skip scan more complex  example",
		map[string]map[string]string{
			MySql9: {
				"default": "select min(min_c2) from (select c1, min(c2) as min_c2 from large_group_by_table group by c1) as t",
			},
			PostgreSql17: {
				"default":     "select min(min_c2) from (select c1, min(c2) as min_c2 from large_group_by_table group by c1) as t",
				"optimised":   "select min (t3.min_c2) from (select distinct c1 from large_group_by_table) as t cross join lateral (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3",
				"optimised-2": "with recursive t as (select min(c1) as c1 from large_group_by_table union all select (select min(c1) from large_group_by_table where c1 > t.c1) from t where t.c1 is not null) select min(t3.min_c2) from t cross join lateral (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3;",
			},
			PostgreSql18: {
				"default":     "select min(min_c2) from (select c1, min(c2) as min_c2 from large_group_by_table group by c1) as t",
				"optimised":   "select min (t3.min_c2) from (select distinct c1 from large_group_by_table) as t cross join lateral (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3",
				"optimised-2": "with recursive t as (select min(c1) as c1 from large_group_by_table union all select (select min(c1) from large_group_by_table where c1 > t.c1) from t where t.c1 is not null) select min(t3.min_c2) from t cross join lateral (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3;",
			},
			MsSql22: {
				"default":               "select min(min_c2) from (select c1, min(c2) as min_c2 from large_group_by_table group by c1) as t",
				"optimised":             "select min(t3.min_c2) from (select distinct(c1) as c1 from large_group_by_table) as t cross apply (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3;",
				"super-optimised":       "select min(t4.min_c2) from (select min(c1) as min_c1, max(c1) as max_c1 from large_group_by_table) as t cross apply (select n.id from numbers as n where n.id >= t.min_c1 and n.id <= t.max_c1) as t2 cross apply (select min(t3.c2) as min_c2 from large_group_by_table as t3 where t3.c1 = t2.id) as t4;",
				"super-super-optimised": "select min(t3.min_c2) from (select 0 as c1 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as t cross apply (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3;",
			},
		},
		QueryInt,
		0,
	},
	"skip-scan-2": {
		"",
		map[string]map[string]string{
			MySql9: {
				"default": "select count(*) from skip_scan_example where b = 0;",
			},
			PostgreSql17: {
				"default": "select count(*) from skip_scan_example where b = 0;",
			},
			PostgreSql18: {
				"default": "select count(*) from skip_scan_example where b = 0;",
			},
			MsSql22: {
				"default": "select count(*) from skip_scan_example where b = 0;",
			},
		},
		QueryInt,
		30,
	},

	"index-merge-opt": {
		"index seek with complex condition",
		map[string]map[string]string{
			MySql9: {
				"a - default":                 "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"b - bigger range":            "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"c - much bigger range":       "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"d - changed predicate order": "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
			PostgreSql17: {
				"a - default":                 "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"b - bigger range":            "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"c - much bigger range":       "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"d - changed predicate order": "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
			MsSql22: {
				"a - default":                 "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"b - bigger range":            "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"c - much bigger range":       "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"d - changed predicate order": "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
		},
		QueryInt,
		200,
	},
	"join-agg": {
		"join and aggregate 2 sorted tables",
		map[string]map[string]string{
			MySql8: {
				"default":         "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from `order` as o inner join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
				"force hash join": "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from `order` as o ignore index (primary) inner join order_detail as od ignore index (primary) on od.order_id = o.id group by o.id) as tmp;",
				"extra pre-agg":   "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od_agg.price) as total_price from `order` as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id) as tmp;",
			},
			MySql9: {
				"default":         "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from `order` as o inner join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
				"force hash join": "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from `order` as o ignore index (primary) inner join order_detail as od ignore index (primary) on od.order_id = o.id group by o.id) as tmp;",
				"extra pre-agg":   "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od_agg.price) as total_price from `order` as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id) as tmp;",
			},
			PostgreSql17: {
				"default":       "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from \"order\" as o inner join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
				"extra pre-agg": "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od_agg.price) as total_price from \"order\" as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id) as tmp;",
			},
			MsSql22: {
				"default":                       "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from [order] as o inner join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
				"default seq":                   "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from [order] as o inner join order_detail as od on od.order_id = o.id group by o.id) as tmp option (maxdop 1);",
				"extra pre-agg":                 "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od_agg.price) as total_price from [order] as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id) as tmp;",
				"merge join":                    "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from [order] as o inner merge join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
				"extra pre-agg with merge join": "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od_agg.price) as total_price from [order] as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id) as tmp;",

				//"loop join":            "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from [order] as o inner loop join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
				//"loop join (maxdop 1)": "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from [order] as o inner loop join order_detail as od on od.order_id = o.id group by o.id) as tmp option (maxdop 1);",
			},
		},
		QueryIntAndFloat64,
		5,
	},
	"join-partial-agg": {
		"grouping with partial aggregation",
		map[string]map[string]string{
			MySql8: {
				"small": "select min(cnt) as a, min(name) as b from (select p.name, count(*) as cnt from `order` as o inner join group_by_table as l on l.id = o.id inner join product as p on p.id = l.c group by p.name) as t;",
				"big":   "select min(cnt) as a, min(name) as b from (select p.name, count(*) as cnt from `order` as o inner join group_by_table as l on l.id = o.id inner join product as p on p.id = l.a group by p.name) as t;",
			},
			PostgreSql17: {
				"small":           "select min(cnt) as a, min(name) as b from (select p.name, count(*) as cnt from \"order\" as o inner join group_by_table as l on l.id = o.id inner join product as p on p.id = l.c group by p.name) as t;",
				"small-optimized": "select min(cnt) as a, min(name) as b from (select p.name, cnt from (select l.c, count(*) as cnt from \"order\" as o inner join group_by_table as l on l.id = o.id group by l.c) as t inner join product as p on p.id = t.c) as t;",
				"big":             "select min(cnt) as a, min(name) as b from (select p.name, count(*) as cnt from \"order\" as o inner join group_by_table as l on l.id = o.id inner join product as p on p.id = l.a group by p.name) as t;",
				"big-optimized":   "select min(cnt) as a, min(name) as b from (select p.name, cnt from (select l.a, count(*) as cnt from \"order\" as o inner join group_by_table as l on l.id = o.id group by l.a) as t inner join product as p on p.id = t.a) as t;",
			},
			MsSql22: {
				"small": "select min(cnt) as a, min(name) as b from (select p.name, count(*) as cnt from [order] as o inner join group_by_table as l on l.id = o.id inner join product as p on p.id = l.c group by p.name) as t;",
				"big":   "select min(cnt) as a, min(name) as b from (select p.name, count(*) as cnt from [order] as o inner join group_by_table as l on l.id = o.id inner join product as p on p.id = l.a group by p.name) as t;",
			},
		},
		QueryIntAndString,
		15,
	},
	"combine-index": {
		"combine select from 2 indexes",
		map[string]map[string]string{
			MySql8: {
				"a - simple":       "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"b - complex":      "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				"c - more complex": "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
			},
			MySql9: {
				"a - simple":       "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"b - complex":      "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				"c - more complex": "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
			},
			PostgreSql16: {
				"a - simple":       "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"b - complex":      "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				"c - more complex": "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
			},
			PostgreSql17: {
				"a - simple":       "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"b - complex":      "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				"c - more complex": "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
			},
			PostgreSql18: {
				"a - simple":       "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"b - complex":      "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				"c - more complex": "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
			},
			MsSql22: {
				"a - simple":            "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"b - complex":           "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				"b - complex-loop-join": "select count(*) from large_group_by_table as l inner loop join large_group_by_table as l2 on l2.id = l.id and (l2.c2 = 1 or l2.c2 = 2 or l2.c2 = 50) where l.c3 = 1;",
				"c - more complex":      "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
				//"x2":           "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 >= 0 and l.c2 < 22 and l.c3 = 1;",
			},
			MsSql25: {
				"a - simple":            "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"b - complex":           "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				"b - complex-loop-join": "select count(*) from large_group_by_table as l inner loop join large_group_by_table as l2 on l2.id = l.id and (l2.c2 = 1 or l2.c2 = 2 or l2.c2 = 50) where l.c3 = 1;",
				"c - more complex":      "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
				//"x2":           "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 >= 0 and l.c2 < 22 and l.c3 = 1;",
			},
		},
		QueryInt,
		300,
	},

	//"needs-refactoring-00-3": {
	//	"count rows in parallel",
	//	map[string]map[string]string{
	//		MySql8: {
	//			"w/o pk":  "select count(*) from filter_1m;",
	//			"pk":      "select count(*) from filter_1m_with_pk;",
	//			"pk - id": "select count(id) from filter_1m_with_pk;",
	//		},
	//		PostgreSql17: {
	//			"w/o pk":    "select count(*) from filter_1m;",
	//			"pk":        "select count(*) from filter_1m_with_pk;",
	//			"pk - id":   "select count(id) from filter_1m_with_pk;",
	//			"4 threads": "set max_parallel_workers_per_gather = 4; select count(*) from filter_1m;",
	//		},
	//		MsSql22: {
	//			"w/o pk":  "select count(*) from filter_1m;",
	//			"pk":      "select count(*) from filter_1m_with_pk;",
	//			"pk - id": "select count(id) from filter_1m_with_pk;",
	//		},
	//	},
	//	QueryInt,
	//	10,
	//},
	//"needs-refactoring-01": {
	//	"lookup by primary key",
	//	map[string]map[string]string{
	//		MySql8: {
	//			"first key": "select id from client as c where id = 0;",
	//			//"middle key":                        "select id from client as c where id = 5000;",
	//			"last key": "select id from client as c where id = 9999;",
	//			//"not existing key at the beginning": "select id from client as c where id = 100000;",
	//			//"not existing key at the end":       "select id from client as c where id = 100000;",
	//			"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
	//		},
	//		PostgreSql17: {
	//			"first key": "select id from client as c where id = 0;",
	//			//"middle key":                        "select id from client as c where id = 5000;",
	//			"last key": "select id from client as c where id = 9999;",
	//			//"not existing key at the beginning": "select id from client as c where id = 100000;",
	//			//"not existing key at the end":       "select id from client as c where id = 100000;",
	//			"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
	//		},
	//		MsSql22: {
	//			"first key": "select id from client as c where id = 0;",
	//			//"middle key":                        "select id from client as c where id = 5000;",
	//			"last key": "select id from client as c where id = 9999;",
	//			//"not existing key at the beginning": "select id from client as c where id = 100000;",
	//			//"not existing key at the end":       "select id from client as c where id = 100000;",
	//			"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
	//		},
	//	},
	//	QueryInt,
	//	3000,
	//},
	//"needs-refactoring-02": {
	//	"lookup by primary key + column not in index",
	//	map[string]map[string]string{
	//		MySql8: {
	//			"": "select id, name from client as c where id = 1;",
	//		},
	//		PostgreSql17: {
	//			"": "select id, name from client as c where id = 1;",
	//		},
	//		MsSql19: {
	//			"": "select id, name from client as c where id = 1;",
	//		},
	//		MsSql22: {
	//			"": "select id, name from client as c where id = 1;",
	//		},
	//	},
	//	QueryIntAndString,
	//	3000,
	//},
	//"needs-refactoring-03": {
	//	"min and max",
	//	map[string]map[string]string{
	//		MySql8: {
	//			"min":     "select min(id) from client as c;",
	//			"max":     "select min(id) from client as c;",
	//			"min-max": "select min(id) + max(id) from client as c;",
	//		},
	//		PostgreSql17: {
	//			"min":     "select min(id) from client as c;",
	//			"max":     "select min(id) from client as c;",
	//			"min-max": "select min(id) + max(id) from client as c;",
	//		},
	//		//MsSql19: {
	//		//	"min":     "select min(id) from client as c;",
	//		//	"max":     "select min(id) from client as c;",
	//		//	"min-max": "select min(id) + max(id) from client as c;",
	//		//},
	//		MsSql22: {
	//			"min":     "select min(id) from client as c;",
	//			"max":     "select min(id) from client as c;",
	//			"min-max": "select min(id) + max(id) from client as c;",
	//		},
	//	},
	//	QueryInt,
	//	3000,
	//},
}

func QueryInt(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	// execute query with context and handle no rows error

	var i int
	if err := db.QueryRowContext(ctx, query).Scan(&i); err != nil {
		if err == sql.ErrNoRows {
			i = -1
		} else {
			log.Fatalf("unable to execute query: %v", err)
		}
	}
	//log.Println("result = ", result)
}

func QueryString(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	// execute query with context and handle no rows error

	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		if err == sql.ErrNoRows {
			s = "N/A"
		} else {
			log.Fatalf("unable to execute query: %v", err)
		}
	}
	//log.Println("result = ", result)
}

func QueryIntAndString(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	var i int
	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&i, &s); err != nil {
		if err == sql.ErrNoRows {
			i = -1
			s = "N/A"
		} else {
			log.Fatalf("unable to execute query: %v", err)
		}
	}
	//log.Println("result = ", result)
}

func QueryTsAndString(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	var t time.Time
	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&t, &s); err != nil {
		if err == sql.ErrNoRows {
			t = time.Now()
			s = "N/A"
		} else {
			log.Fatalf("unable to execute query: %v", err)
		}
	}
	//log.Println("result = ", result)
}

func QueryIntAndFloat64(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	var i int
	var f float64
	if err := db.QueryRowContext(ctx, query).Scan(&i, &f); err != nil {
		if err == sql.ErrNoRows {
			i = -1
			f = 0.0
		} else {
			log.Fatalf("unable to execute query: %v", err)
		}
	}
	//log.Println("result = ", result)
}
