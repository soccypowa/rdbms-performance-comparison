package demo

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type testData struct {
	testName  string
	queries   map[string]map[string]string
	f         func(context.Context, *sql.DB, string)
	execCount int
}

var tests = []testData{
	//{
	//	"01 - lookup by primary key",
	//	map[string]map[string]string{
	//		"mysql": {
	//			//"first key":                         "select id from client as c where id = 0;",
	//			//"middle key":                        "select id from client as c where id = 5000;",
	//			//"last key":                          "select id from client as c where id = 9999;",
	//			//"not existing key at the beginning": "select id from client as c where id = 100000;",
	//			//"not existing key at the end":       "select id from client as c where id = 100000;",
	//			"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
	//		},
	//		"postgres": {
	//			//"first key":                         "select id from client as c where id = 0;",
	//			//"middle key":                        "select id from client as c where id = 5000;",
	//			//"last key":                          "select id from client as c where id = 9999;",
	//			//"not existing key at the beginning": "select id from client as c where id = 100000;",
	//			//"not existing key at the end":       "select id from client as c where id = 100000;",
	//			"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
	//		},
	//		"mssql-22": {
	//			//"first key":                         "select id from client as c where id = 0;",
	//			//"middle key":                        "select id from client as c where id = 5000;",
	//			//"last key":                          "select id from client as c where id = 9999;",
	//			//"not existing key at the beginning": "select id from client as c where id = 100000;",
	//			//"not existing key at the end":       "select id from client as c where id = 100000;",
	//			"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
	//		},
	//		"mssql-19": {
	//			//"first key":                         "select id from client as c where id = 0;",
	//			//"middle key":                        "select id from client as c where id = 5000;",
	//			//"last key":                          "select id from client as c where id = 9999;",
	//			//"not existing key at the beginning": "select id from client as c where id = 100000;",
	//			//"not existing key at the end":       "select id from client as c where id = 100000;",
	//			"lookup_and_agg": "select count(*) from order_detail as od where order_id = 1;",
	//		},
	//	},
	//	QueryInt,
	//	3000,
	//},
	//{
	//	"02 - lookup by primary key + column not in index",
	//	map[string]map[string]string{
	//		"mysql": {
	//			"": "select id, name from client as c where id = 1;",
	//		},
	//		"postgres": {
	//			"": "select id, name from client as c where id = 1;",
	//		},
	//		"mssql2022": {
	//			"": "select id, name from client as c where id = 1;",
	//		},
	//	},
	//	QueryIntAndString,
	//	3000,
	//},
	//{
	//	"03 - min and max",
	//	map[string]map[string]string{
	//		"mysql": {
	//			"min":     "select min(id) from client as c;",
	//			"max":     "select min(id) from client as c;",
	//			"min-max": "select min(id) + max(id) from client as c;",
	//		},
	//		"postgres": {
	//			"min":     "select min(id) from client as c;",
	//			"max":     "select min(id) from client as c;",
	//			"min-max": "select min(id) + max(id) from client as c;",
	//		},
	//		"mssql2022": {
	//			"min":     "select min(id) from client as c;",
	//			"max":     "select min(id) from client as c;",
	//			"min-max": "select min(id) + max(id) from client as c;",
	//		},
	//	},
	//	QueryInt,
	//	3000,
	//},
	{
		"04 - index seek with complex condition",
		map[string]map[string]string{
			"mysql": {
				"":                  "select count(*) from client where id >= 1 and id < 10000 and id > 9990;",
				"bigger range":      "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range": "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
			},
			"postgres": {
				"":                  "select count(*) from client where id >= 1 and id < 10000 and id > 9990;",
				"bigger range":      "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range": "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
			},
			"mssql-22": {
				"":                  "select count(*) from client where id >= 1 and id < 10000 and id > 9990;",
				"bigger range":      "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range": "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
			},
			"mssql-19": {
				"":                  "select count(*) from client where id >= 1 and id < 10000 and id > 9990;",
				"bigger range":      "select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;",
				"much bigger range": "select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;",
			},
		},
		QueryInt,
		200,
	},
	//{
	//	"05",
	//	map[string]map[string]string{
	//		"mysql": {
	//			"": "select count(*) from `order` as o inner join `order_detail` as od on od.order_id = o.id;",
	//		},
	//		"postgres": {
	//			"":             "select count(*) from \"order\" as o inner join order_detail as od on od.order_id = o.id;",
	//			"hashjoin=off": "SET enable_hashjoin = off; select count(*) from \"order\" as o inner join order_detail as od on od.order_id = o.id;",
	//		},
	//		"mssql2022": {
	//			"":          "select count(*) from [order] as o inner join order_detail as od on od.order_id = o.id;",
	//			"loop join": "select count(*) from [order] as o inner loop join order_detail as od on od.order_id = o.id;",
	//		},
	//	},
	//	Query05,
	//	0,
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
