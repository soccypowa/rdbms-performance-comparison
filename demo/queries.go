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
	"00-1": {
		"filter about 90% of 10 million rows table",
		map[string]map[string]string{
			MySql: {
				//"tinyint": "select count(*) from filter_1m where status_id_tinyint = 1;",
				"int":           "select count(*) from filter_1m where status_id_int = 1;",
				"int w/o where": "select count(*) from filter_1m",
				"char":          "select count(*) from filter_1m where status_char = 'active';",
				"varchar":       "select count(*) from filter_1m where status_varchar = 'active';",
				"text":          "select count(*) from filter_1m where status_text = 'active';",
			},
			PostgreSql: {
				//"tinyint": "select count(*) from filter_1m where status_id_tinyint = 1;",
				"int":     "select count(*) from filter_1m where status_id_int = 1;",
				"char":    "select count(*) from filter_1m where status_char = 'active';",
				"varchar": "select count(*) from filter_1m where status_varchar = 'active';",
				"text":    "select count(*) from filter_1m where status_text = 'active';",
			},
			//MsSql19: {
			//	//"tinyint": "select count(*) from filter_1m where status_id_tinyint = 1;",
			//	"int":     "select count(*) from filter_1m where status_id_int = 1;",
			//	"char":    "select count(*) from filter_1m where status_char = 'active';",
			//	"varchar": "select count(*) from filter_1m where status_varchar = 'active';",
			//	"text":    "select count(*) from filter_1m where status_text = 'active';",
			//},
			MsSql22: {
				//"tinyint": "select count(*) from filter_1m where status_id_tinyint = 1;",
				"int":     "select count(*) from filter_1m where status_id_int = 1;",
				"char":    "select count(*) from filter_1m where status_char = 'active';",
				"varchar": "select count(*) from filter_1m where status_varchar = 'active';",
				"text":    "select count(*) from filter_1m where status_text = 'active';",
			},
		},
		QueryInt,
		10,
	},
	"00-2": {
		"filter 10 million rows table",
		map[string]map[string]string{
			MySql: {
				"10%": "select count(*) from filter_1m where status_id_int = 0;",
				"90%": "select count(*) from filter_1m where status_id_int = 1;",
			},
			PostgreSql: {
				"10%": "select count(*) from filter_1m where status_id_int = 0;",
				"90%": "select count(*) from filter_1m where status_id_int = 1;",
			},
			MsSql22: {
				"10%": "select count(*) from filter_1m where status_id_int = 0;",
				"90%": "select count(*) from filter_1m where status_id_int = 1;",
			},
		},
		QueryInt,
		10,
	},
	"00-3": {
		"count rows in parallel",
		map[string]map[string]string{
			MySql: {
				"w/o pk":  "select count(*) from filter_1m;",
				"pk":      "select count(*) from filter_1m_with_pk;",
				"pk - id": "select count(id) from filter_1m_with_pk;",
			},
			PostgreSql: {
				"w/o pk":    "select count(*) from filter_1m;",
				"pk":        "select count(*) from filter_1m_with_pk;",
				"pk - id":   "select count(id) from filter_1m_with_pk;",
				"4 threads": "set max_parallel_workers_per_gather = 4; select count(*) from filter_1m;",
			},
			MsSql22: {
				"w/o pk":  "select count(*) from filter_1m;",
				"pk":      "select count(*) from filter_1m_with_pk;",
				"pk - id": "select count(id) from filter_1m_with_pk;",
			},
		},
		QueryInt,
		10,
	},
	"01": {
		"lookup by primary key",
		map[string]map[string]string{
			MySql: {
				"first key": "select id from client as c where id = 0;",
				//"middle key":                        "select id from client as c where id = 5000;",
				"last key": "select id from client as c where id = 9999;",
				//"not existing key at the beginning": "select id from client as c where id = 100000;",
				//"not existing key at the end":       "select id from client as c where id = 100000;",
				"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
			},
			PostgreSql: {
				"first key": "select id from client as c where id = 0;",
				//"middle key":                        "select id from client as c where id = 5000;",
				"last key": "select id from client as c where id = 9999;",
				//"not existing key at the beginning": "select id from client as c where id = 100000;",
				//"not existing key at the end":       "select id from client as c where id = 100000;",
				"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
			},
			MsSql22: {
				"first key": "select id from client as c where id = 0;",
				//"middle key":                        "select id from client as c where id = 5000;",
				"last key": "select id from client as c where id = 9999;",
				//"not existing key at the beginning": "select id from client as c where id = 100000;",
				//"not existing key at the end":       "select id from client as c where id = 100000;",
				"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
			},
		},
		QueryInt,
		3000,
	},
	"02": {
		"lookup by primary key + column not in index",
		map[string]map[string]string{
			MySql: {
				"": "select id, name from client as c where id = 1;",
			},
			PostgreSql: {
				"": "select id, name from client as c where id = 1;",
			},
			MsSql19: {
				"": "select id, name from client as c where id = 1;",
			},
			MsSql22: {
				"": "select id, name from client as c where id = 1;",
			},
		},
		QueryIntAndString,
		3000,
	},
	"03": {
		"min and max",
		map[string]map[string]string{
			MySql: {
				"min":     "select min(id) from client as c;",
				"max":     "select min(id) from client as c;",
				"min-max": "select min(id) + max(id) from client as c;",
			},
			PostgreSql: {
				"min":     "select min(id) from client as c;",
				"max":     "select min(id) from client as c;",
				"min-max": "select min(id) + max(id) from client as c;",
			},
			//MsSql19: {
			//	"min":     "select min(id) from client as c;",
			//	"max":     "select min(id) from client as c;",
			//	"min-max": "select min(id) + max(id) from client as c;",
			//},
			MsSql22: {
				"min":     "select min(id) from client as c;",
				"max":     "select min(id) from client as c;",
				"min-max": "select min(id) + max(id) from client as c;",
			},
		},
		QueryInt,
		3000,
	},
	"04": {
		"index seek with complex condition",
		map[string]map[string]string{
			MySql: {
				"":                  "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"bigger range":      "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range": "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"fixed":             "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
			PostgreSql: {
				"":                  "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"bigger range":      "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range": "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"fixed":             "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
			//MsSql19: {
			//	"":                  "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
			//	"bigger range":      "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
			//	"much bigger range": "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
			//	"fixed":             "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			//},
			MsSql22: {
				"":                  "select count(*) from client where id >= 1 and id < 10000 and id < 2;",
				"bigger range":      "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range": "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
				"fixed":             "select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;",
			},
		},
		QueryInt,
		200,
	},
	"05": {
		"nonclustered index seek vs. scan",
		map[string]map[string]string{
			MySql: {
				"1 row":     "select count(name) from client where country = 'UK';",
				"9 rows":    "select count(name) from client where country = 'NL';",
				"90 rows":   "select count(name) from client where country = 'FR';",
				"900 rows":  "select count(name) from client where country = 'CY';",
				"4000 rows": "select count(name) from client where country = 'US';",
				"7333 rows": "select count(name) from client where country >= 'US';",
			},
			PostgreSql: {
				"1 row":     "select count(name) from client where country = 'UK';",
				"9 rows":    "select count(name) from client where country = 'NL';",
				"90 rows":   "select count(name) from client where country = 'FR';",
				"900 rows":  "select count(name) from client where country = 'CY';",
				"4000 rows": "select count(name) from client where country = 'US';",
				"7333 rows": "select count(name) from client where country >= 'US';",
			},
			//MsSql19: {
			//	"1 row":     "select count(name) from client where country = 'UK';",
			//	"9 rows":    "select count(name) from client where country = 'NL';",
			//	"90 rows":   "select count(name) from client where country = 'FR';",
			//	"900 rows":  "select count(name) from client where country = 'CY';",
			//	"4000 rows": "select count(name) from client where country = 'US';",
			//	"7333 rows": "select count(name) from client where country >= 'US';",
			//	//"forceseek - 90 rows":    "select min(name) from client with (forceseek) where country = 'FR';",
			//	//"forceseek - 900 rows": "select min(name) from client with (forceseek) where country = 'CY';",
			//	//"forceseek - 4000 rows": "select min(name) from client with (forceseek) where country = 'US';",
			//},
			MsSql22: {
				"1 row":     "select count(name) from client where country = 'UK';",
				"9 rows":    "select count(name) from client where country = 'NL';",
				"90 rows":   "select count(name) from client where country = 'FR';",
				"900 rows":  "select count(name) from client where country = 'CY';",
				"4000 rows": "select count(name) from client where country = 'US';",
				"7333 rows": "select count(name) from client where country >= 'US';",
				//"forceseek - 90 rows":    "select min(name) from client with (forceseek) where country = 'FR';",
				//"forceseek - 900 rows": "select min(name) from client with (forceseek) where country = 'CY';",
				//"forceseek - 4000 rows": "select min(name) from client with (forceseek) where country = 'US';",
			},
		},
		QueryString,
		200,
	},
	"05-min": {
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
				"1 row":     "select min(name) from client where country = 'UK';",
				"9 rows":    "select min(name) from client where country = 'NL';",
				"90 rows":   "select min(name) from client where country = 'FR';",
				"900 rows":  "select min(name) from client where country = 'CY';",
				"4000 rows": "select min(name) from client where country = 'US';",
				"7333 rows": "select min(name) from client where country >= 'US';",
				//"forceseek - 90 rows":    "select min(name) from client with (forceseek) where country = 'FR';",
				//"forceseek - 900 rows": "select min(name) from client with (forceseek) where country = 'CY';",
				//"forceseek - 4000 rows": "select min(name) from client with (forceseek) where country = 'US';",
			},
		},
		QueryString,
		200,
	},
	"05-min-large": {
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
		10,
	},
	"06": {
		"join 2 sorted tables",
		map[string]map[string]string{
			MySql: {
				"client-ex":    "select count(*) from client as c inner join client_ex as c_ex on c_ex.id = c.id;",
				"order-detail": "select count(*) from `order` as o inner join `order_detail` as od on od.order_id = o.id;",
				"pre-agg":      "select count(*) from `order` as o inner join (select order_id from order_detail group by order_id) as od on od.order_id = o.id;",
			},
			PostgreSql: {
				"client-ex":     "select count(*) from client as c inner join client_ex as c_ex on c_ex.id = c.id;",
				"order-detail":  "select count(*) from \"order\" as o inner join order_detail as od on od.order_id = o.id;",
				"hashjoin=off":  "set enable_hashjoin = off; select count(*) from \"order\" as o inner join order_detail as od on od.order_id = o.id; set enable_hashjoin = on;",
				"pre-agg":       "select count(*) from \"order\" as o inner join (select order_id from order_detail group by order_id) as od on od.order_id = o.id;",
				"pre-agg-index": "select count(*) from \"order\" as o inner join (select order_id, product_id from order_detail group by order_id, product_id) as od on od.order_id = o.id;",
			},
			MsSql19: {
				"client-ex":    "select count(*) from client as c inner join client_ex as c_ex on c_ex.id = c.id;",
				"order-detail": "select count(*) from [order] as o inner join order_detail as od on od.order_id = o.id;",
				"loop join":    "select count(*) from [order] as o inner loop join order_detail as od on od.order_id = o.id;",
				"pre-agg":      "select count(*) from [order] as o inner join (select order_id from order_detail group by order_id) as od on od.order_id = o.id;",
			},
			MsSql22: {
				"client-ex":    "select count(*) from client as c inner join client_ex as c_ex on c_ex.id = c.id;",
				"order-detail": "select count(*) from [order] as o inner join order_detail as od on od.order_id = o.id;",
				"loop join":    "select count(*) from [order] as o inner loop join order_detail as od on od.order_id = o.id;",
				"pre-agg":      "select count(*) from [order] as o inner join (select order_id from order_detail group by order_id) as od on od.order_id = o.id;",
			},
		},
		QueryInt,
		0,
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
	"skip_scan": {
		"group by",
		map[string]map[string]string{
			MySql: {
				"a": "select count(*) from (select a from group_by_table group by a) as tmp",
				"b": "select count(*) from (select b from group_by_table group by b) as tmp",
				"c": "select count(*) from (select c from group_by_table group by c) as tmp",
			},
			PostgreSql: {
				"a":          "select count(*) from (select a from group_by_table group by a) as tmp",
				"b":          "set max_parallel_workers_per_gather = 1; select count(*) from (select b from group_by_table group by b) as tmp",
				"b-parallel": "select count(*) from (select b from group_by_table group by b) as tmp",
				"c":          "set max_parallel_workers_per_gather = 1; select count(*) from (select c from group_by_table group by c) as tmp",
				"c-parallel": "select count(*) from (select c from group_by_table group by c) as tmp",
				"a1":         "WITH RECURSIVE t AS (SELECT min(a) AS x FROM group_by_table UNION ALL SELECT (SELECT min(a) FROM group_by_table WHERE a > t.x) FROM t WHERE t.x IS NOT NULL) select count(*) from (SELECT x FROM t WHERE x IS NOT NULL UNION ALL SELECT null WHERE EXISTS (SELECT 1 FROM group_by_table WHERE a IS NULL)) as tmp;",
				"b1":         "WITH RECURSIVE t AS (SELECT min(b) AS x FROM group_by_table UNION ALL SELECT (SELECT min(b) FROM group_by_table WHERE b > t.x) FROM t WHERE t.x IS NOT NULL) select count(*) from (SELECT x FROM t WHERE x IS NOT NULL UNION ALL SELECT null WHERE EXISTS (SELECT 1 FROM group_by_table WHERE b IS NULL)) as tmp;",
				"c1":         "WITH RECURSIVE t AS (SELECT min(c) AS x FROM group_by_table UNION ALL SELECT (SELECT min(c) FROM group_by_table WHERE c > t.x) FROM t WHERE t.x IS NOT NULL) select count(*) from (SELECT x FROM t WHERE x IS NOT NULL UNION ALL SELECT null WHERE EXISTS (SELECT 1 FROM group_by_table WHERE c IS NULL)) as tmp;",
			},
			MsSql22: {
				"a":  "select count(*) from (select a from group_by_table group by a) as tmp",
				"b":  "select count(*) from (select b from group_by_table group by b) as tmp",
				"c":  "select count(*) from (select c from group_by_table group by c) as tmp",
				"a1": "create table #result (x int); declare @current int; select top (1) @current = a from group_by_table order by a; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = a from group_by_table where a > @current order by a; end; select count(*) from #result;",
				"b1": "create table #result (x int); declare @current int; select top (1) @current = b from group_by_table order by b; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = b from group_by_table where b > @current order by b; end; select count(*) from #result;",
				"c1": "create table #result (x int); declare @current int; select top (1) @current = c from group_by_table order by c; while @@rowcount > 0 begin insert into #result values (@current); select top (1) @current = c from group_by_table where c > @current order by c; end; select count(*) from #result;",
			},
		},
		QueryInt,
		20,
	},
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
