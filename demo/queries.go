package main

import (
	"context"
	"database/sql"
	"log"
	"time"
)

const MySql = "mysql"
const PostgreSql = "postgres"
const MsSql19 = "mssql-19"
const MsSql22 = "mssql-22"

type testData struct {
	testName  string
	queries   map[string]map[string]string
	f         func(context.Context, *sql.DB, string)
	execCount int
}

var Tests = map[string]testData{
	"01": {
		"select distinct / count distinct",
		map[string]map[string]string{
			MySql: {
				"a":              "select count(distinct a) as cnt from group_by_table",
				"b":              "select count(distinct b) as cnt from group_by_table",
				"c":              "select count(distinct c) as cnt from group_by_table",
				"c-no-skip-scan": "select /*+ NO_SKIP_SCAN(group_by_table idx_group_by_table_c) */ count(distinct c) as cnt from group_by_table",
			},
			//PostgreSql: {
			//	"a":          "select count(distinct a) as cnt from group_by_table",
			//	"b":          "set max_parallel_workers_per_gather = 1; select count(distinct b) as cnt from group_by_table",
			//	"b-parallel": "select count(distinct b) as cnt from group_by_table",
			//	"c":          "set max_parallel_workers_per_gather = 1; select count(distinct c) as cnt from group_by_table",
			//	"c-parallel": "select count(distinct c) as cnt from group_by_table",
			//	"a1":         "with recursive t as (select min(a) as x from group_by_table union all select (select min(a) from group_by_table where a > t.x) from t where t.x is not null) select count(*) from (select x from t where x is not null union all select null where exists (select 1 from group_by_table where a is null)) as tmp;",
			//	"b1":         "with recursive t as (select min(b) as x from group_by_table union all select (select min(b) from group_by_table where b > t.x) from t where t.x is not null) select count(*) from (select x from t where x is not null union all select null where exists (select 1 from group_by_table where b is null)) as tmp;",
			//	"c1":         "with recursive t as (select min(c) as x from group_by_table union all select (select min(c) from group_by_table where c > t.x) from t where t.x is not null) select count(*) from (select x from t where x is not null union all select null where exists (select 1 from group_by_table where c is null)) as tmp;",
			//},
			//MsSql22: {
			//	"a":  "select count(distinct a) as cnt from group_by_table",
			//	"b":  "select count(distinct b) as cnt from group_by_table",
			//	"c":  "select count(distinct c) as cnt from group_by_table",
			//	"a1": "create table #result (x int); declare @current int; select top (1) @current = a from group_by_table order by a; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = a from group_by_table where a > @current order by a; end; select count(*) from #result;",
			//	"b1": "create table #result (x int); declare @current int; select top (1) @current = b from group_by_table order by b; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = b from group_by_table where b > @current order by b; end; select count(*) from #result;",
			//	"c1": "create table #result (x int); declare @current int; select top (1) @current = c from group_by_table order by c; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = c from group_by_table where c > @current order by c; end; select count(*) from #result;",
			//	"a2": "with min_max as (select min(a) as min_a, max(a) as max_a from group_by_table), possible_values as (select n.id from numbers as n inner join min_max as mm on n.id >= mm.min_a and n.id <= mm.max_a), result as (select pv.id from possible_values as pv where exists (select top (1) 1 from group_by_table as g where g.a = pv.id)) select count(*) from result;",
			//	"b2": "with min_max as (select min(b) as min_b, max(b) as max_b from group_by_table), possible_values as (select n.id from numbers as n inner join min_max as mm on n.id >= mm.min_b and n.id <= mm.max_b), result as (select pv.id from possible_values as pv where exists (select top (1) 1 from group_by_table as g where g.b = pv.id)) select count(*) from result;",
			//	"c2": "with min_max as (select min(c) as min_c, max(c) as max_c from group_by_table), possible_values as (select n.id from numbers as n inner join min_max as mm on n.id >= mm.min_c and n.id <= mm.max_c), result as (select pv.id from possible_values as pv where exists (select top (1) 1 from group_by_table as g where g.c = pv.id)) select count(*) from result;",
			//},
		},
		QueryInt,
		20,
	},
	"02": {
		"index seek with complex condition",
		map[string]map[string]string{
			MySql: {
				"default":                 "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"bigger range":            "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range":       "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"changed predicate order": "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
			PostgreSql: {
				"default":                 "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"bigger range":            "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range":       "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"changed predicate order": "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
			MsSql22: {
				"default":                 "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"bigger range":            "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range":       "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"changed predicate order": "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
		},
		QueryInt,
		200,
	},
	"03": {
		"nonclustered index seek vs. scan",
		map[string]map[string]string{
			MySql: {
				"1 row":     "select min(name) from client where country = 'UK';",
				"9 rows":    "select min(name) from client where country = 'NL';",
				"90 rows":   "select min(name) from client where country = 'FR';",
				"900 rows":  "select min(name) from client where country = 'CY';",
				"4000 rows": "select min(name) from client where country = 'US';",
				"7333 rows": "select min(name) from client where country >= 'US';",
			},
			PostgreSql: {
				"1 row":     "select min(name) from client where country = 'UK';",
				"9 rows":    "select min(name) from client where country = 'NL';",
				"90 rows":   "select min(name) from client where country = 'FR';",
				"900 rows":  "select min(name) from client where country = 'CY';",
				"4000 rows": "select min(name) from client where country = 'US';",
				"7333 rows": "select min(name) from client where country >= 'US';",
			},
			MsSql22: {
				"1 row":                 "select min(name) from client where country = 'UK';",
				"9 rows":                "select min(name) from client where country = 'NL';",
				"90 rows":               "select min(name) from client where country = 'FR';",
				"900 rows":              "select min(name) from client where country = 'CY';",
				"4000 rows":             "select min(name) from client where country = 'US';",
				"7333 rows":             "select min(name) from client where country >= 'US';",
				"forceseek - 90 rows":   "select min(name) from client with (forceseek) where country = 'FR';",
				"forceseek - 900 rows":  "select min(name) from client with (forceseek) where country = 'CY';",
				"forceseek - 4000 rows": "select min(name) from client with (forceseek) where country = 'US';",
			},
		},
		QueryString,
		200,
	},
	"03-large": {
		"nonclustered index seek vs. scan",
		map[string]map[string]string{
			MySql: {
				"100 row":     "select min(name) from client_large where country = 'UK';",
				"900 rows":    "select min(name) from client_large where country = 'NL';",
				"9,000 rows":  "select min(name) from client_large where country = 'FR';",
				"90,000 rows": "select min(name) from client_large where country = 'CY';",
				//"400,000 rows": "select min(name) from client_large where country = 'US';",
				//"733,333 rows": "select min(name) from client_large where country >= 'US';",
			},
			PostgreSql: {
				"100 row":      "select min(name) from client_large where country = 'UK';",
				"900 rows":     "select min(name) from client_large where country = 'NL';",
				"9,000 rows":   "select min(name) from client_large where country = 'FR';",
				"90,000 rows":  "select min(name) from client_large where country = 'CY';",
				"400,000 rows": "select min(name) from client_large where country = 'US';",
				"733,333 rows": "select min(name) from client_large where country >= 'US';",
			},
			MsSql22: {
				"100 row":      "select min(name) from client_large where country = 'UK';",
				"900 rows":     "select min(name) from client_large where country = 'NL';",
				"9,000 rows":   "select min(name) from client_large where country = 'FR';",
				"90,000 rows":  "select min(name) from client_large where country = 'CY';",
				"400,000 rows": "select min(name) from client_large where country = 'US';",
				"733,333 rows": "select min(name) from client_large where country >= 'US';",
				//"forceseek - 90 rows":    "select min(name) from client with (forceseek) where country = 'FR';",
				//"forceseek - 900 rows": "select min(name) from client with (forceseek) where country = 'CY';",
				//"forceseek - 4000 rows": "select min(name) from client with (forceseek) where country = 'US';",
			},
		},
		QueryString,
		20,
	},
	"04": {
		"join and aggregate 2 sorted tables",
		map[string]map[string]string{
			MySql: {
				"":                "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from `order` as o inner join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
				"force hash join": "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from `order` as o ignore index (primary) inner join order_detail as od ignore index (primary) on od.order_id = o.id group by o.id) as tmp;",
				"extra pre-agg":   "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od_agg.price) as total_price from `order` as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id) as tmp;",
			},
			//PostgreSql: {
			//	"":              "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from \"order\" as o inner join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
			//	"extra pre-agg": "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od_agg.price) as total_price from \"order\" as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id) as tmp;",
			//},
			//MsSql22: {
			//	"":                     "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from [order] as o inner join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
			//	"extra pre-agg":        "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od_agg.price) as total_price from [order] as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id) as tmp;",
			//	"loop join":            "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from [order] as o inner loop join order_detail as od on od.order_id = o.id group by o.id) as tmp;",
			//	"loop join (maxdop 1)": "select min(order_id), sum(total_price) from (select o.id as order_id, sum(od.price) as total_price from [order] as o inner loop join order_detail as od on od.order_id = o.id group by o.id) as tmp option (maxdop 1);",
			//},
		},
		QueryIntAndFloat64,
		20,
	},

	"07": {
		"grouping",
		map[string]map[string]string{
			MySql: {
				"default":     "select min(min_product_id) from (select order_id, min(product_id) as min_product_id from order_detail group by order_id) as t;",
				"large table": "select min(min_c2) from (select c1, min(c2) as min_c2 from large_group_by_table group by c1) as t",
			},
			PostgreSql: {
				"default":     "select min(min_product_id) from (select order_id, min(product_id) as min_product_id from order_detail group by order_id) as t;",
				"large table": "select min(min_c2) from (select c1, min(c2) as min_c2 from large_group_by_table group by c1) as t",
			},
			MsSql22: {
				//"optimized":       "select min(t3.min_c2)\nfrom (select distinct(c1) as c1 from large_group_by_table) as t\ncross apply (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3;",
				"super-optimized": "select min(t4.min_c2)\nfrom (select min(c1) as min_c1, max(c1) as max_c1 from large_group_by_table) as t\ncross apply (select n.id from numbers as n where n.id >= t.min_c1 and n.id <= t.max_c1) as t2\ncross apply (select min(t3.c2) as min_c2 from large_group_by_table as t3 where t3.c1 = t2.id) as t4;\n",
				//"super-super-optimized": "select min(t3.min_c2)\nfrom (select 0 as c1 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as t\ncross apply (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3;",
				"default":     "select min(min_product_id) from (select order_id, min(product_id) as min_product_id from order_detail group by order_id) as t;",
				"large table": "select min(min_c2) from (select c1, min(c2) as min_c2 from large_group_by_table group by c1) as t",
			},
		},
		QueryInt,
		0,
	},
	"08": {
		"grouping with partial aggregation",
		map[string]map[string]string{
			MySql: {
				"small": "select count(*) from (select p.name, count(*) from `order` as o inner join large_group_by_table as l on l.id = o.id inner join product as p on p.id = l.c1 group by p.name) as t;",
				"big":   "select count(*) from (select p.name, count(*) from `order` as o inner join large_group_by_table as l on l.id = o.id inner join product as p on p.id = l.c4 group by p.name) as t;",
			},
			PostgreSql: {
				"small":         "select count(*) from (select p.name, count(*) from \"order\" as o inner join large_group_by_table as l on l.id = o.id inner join product as p on p.id = l.c1 group by p.name) as t;",
				"big":           "select count(*) from (select p.name, count(*) from \"order\" as o inner join large_group_by_table as l on l.id = o.id inner join product as p on p.id = l.c4 group by p.name) as t;",
				"big-optimized": "select count(*)\nfrom (\n    select p.name, cnt\n    from (select l.c1, count(*) as cnt\n          from \"order\" as o\n                   inner join large_group_by_table as l on l.id = o.id group by l.c1) as t\n    inner join product as p on p.id = t.c1\n) as t;\n",
			},
			MsSql22: {
				"small": "select count(*) from (select p.name, count(*) as cnt from [order] as o inner join large_group_by_table as l on l.id = o.id inner join product as p on p.id = l.c1 group by p.name) as t;",
				"big":   "select count(*) from (select p.name, count(*) as cnt from [order] as o inner join large_group_by_table as l on l.id = o.id inner join product as p on p.id = l.c4 group by p.name) as t;",
			},
		},
		QueryInt,
		0,
	},
	"09": {
		"combine select from 2 indexes",
		map[string]map[string]string{
			MySql: {
				"simple":  "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"complex": "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				//"more complex": "select count(*)\nfrom large_group_by_table as l\nwhere (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
			},
			PostgreSql: {
				"simple":  "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"complex": "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				//"more complex": "select count(*)\nfrom large_group_by_table as l\nwhere (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
			},
			MsSql22: {
				"simple":  "select count(*) from large_group_by_table as l where l.c2 = 1 and l.c3 = 1;",
				"complex": "select count(*) from large_group_by_table as l where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;",
				//"more complex": "select count(*)\nfrom large_group_by_table as l\nwhere (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;",
				//"x":            "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;",
				//"x2":           "select count(*)\nfrom large_group_by_table as l\nwhere l.c2 >= 0 and l.c2 < 22 and l.c3 = 1;",
			},
		},
		QueryInt,
		300,
	},

	//"needs-refactoring-00-1": {
	//	"filter about 90% of 10 million rows table",
	//	map[string]map[string]string{
	//		MySql: {
	//			//"tinyint": "select count(*) from filter_1m where status_id_tinyint = 1;",
	//			"int":           "select count(*) from filter_1m where status_id_int = 1;",
	//			"int w/o where": "select count(*) from filter_1m",
	//			"char":          "select count(*) from filter_1m where status_char = 'active';",
	//			"varchar":       "select count(*) from filter_1m where status_varchar = 'active';",
	//			"text":          "select count(*) from filter_1m where status_text = 'active';",
	//		},
	//		PostgreSql: {
	//			//"tinyint": "select count(*) from filter_1m where status_id_tinyint = 1;",
	//			"int":     "select count(*) from filter_1m where status_id_int = 1;",
	//			"char":    "select count(*) from filter_1m where status_char = 'active';",
	//			"varchar": "select count(*) from filter_1m where status_varchar = 'active';",
	//			"text":    "select count(*) from filter_1m where status_text = 'active';",
	//		},
	//		//MsSql19: {
	//		//	//"tinyint": "select count(*) from filter_1m where status_id_tinyint = 1;",
	//		//	"int":     "select count(*) from filter_1m where status_id_int = 1;",
	//		//	"char":    "select count(*) from filter_1m where status_char = 'active';",
	//		//	"varchar": "select count(*) from filter_1m where status_varchar = 'active';",
	//		//	"text":    "select count(*) from filter_1m where status_text = 'active';",
	//		//},
	//		MsSql22: {
	//			//"tinyint": "select count(*) from filter_1m where status_id_tinyint = 1;",
	//			"int":     "select count(*) from filter_1m where status_id_int = 1;",
	//			"char":    "select count(*) from filter_1m where status_char = 'active';",
	//			"varchar": "select count(*) from filter_1m where status_varchar = 'active';",
	//			"text":    "select count(*) from filter_1m where status_text = 'active';",
	//		},
	//	},
	//	QueryInt,
	//	10,
	//},
	//"needs-refactoring-00-2": {
	//	"filter 10 million rows table",
	//	map[string]map[string]string{
	//		MySql: {
	//			"10%": "select count(*) from filter_1m where status_id_int = 0;",
	//			"90%": "select count(*) from filter_1m where status_id_int = 1;",
	//		},
	//		PostgreSql: {
	//			"10%": "select count(*) from filter_1m where status_id_int = 0;",
	//			"90%": "select count(*) from filter_1m where status_id_int = 1;",
	//		},
	//		MsSql22: {
	//			"10%": "select count(*) from filter_1m where status_id_int = 0;",
	//			"90%": "select count(*) from filter_1m where status_id_int = 1;",
	//		},
	//	},
	//	QueryInt,
	//	10,
	//},
	//"needs-refactoring-00-3": {
	//	"count rows in parallel",
	//	map[string]map[string]string{
	//		MySql: {
	//			"w/o pk":  "select count(*) from filter_1m;",
	//			"pk":      "select count(*) from filter_1m_with_pk;",
	//			"pk - id": "select count(id) from filter_1m_with_pk;",
	//		},
	//		PostgreSql: {
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
	//		MySql: {
	//			"first key": "select id from client as c where id = 0;",
	//			//"middle key":                        "select id from client as c where id = 5000;",
	//			"last key": "select id from client as c where id = 9999;",
	//			//"not existing key at the beginning": "select id from client as c where id = 100000;",
	//			//"not existing key at the end":       "select id from client as c where id = 100000;",
	//			"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
	//		},
	//		PostgreSql: {
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
	//		MySql: {
	//			"": "select id, name from client as c where id = 1;",
	//		},
	//		PostgreSql: {
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
	//		MySql: {
	//			"min":     "select min(id) from client as c;",
	//			"max":     "select min(id) from client as c;",
	//			"min-max": "select min(id) + max(id) from client as c;",
	//		},
	//		PostgreSql: {
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
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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

func QueryIntAndFloat64(ctx context.Context, db *sql.DB, query string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
