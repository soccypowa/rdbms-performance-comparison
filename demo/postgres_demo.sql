-- 01 - lookup by primary key
explain analyze select id from client where id = 1;
explain analyze select id from client where id = 100000;

explain analyze select count(*) from order_detail as od where order_id = 1;

-- 02 - lookup by primary key + column not in index
explain analyze select id, name from client where id = 1;
explain analyze select id, name from client where id = 100000;

-- 03 - min and max
explain analyze select min(id) from client;
explain analyze select max(id) from client;
explain analyze select min(id) + max(id) from client;

-- 04 - index seek with complex condition
explain analyze select id, name from client where id >= 1 and id < 10000 and id > 9990;
explain analyze select count(*), '' from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;

-- prepare stmt(int, int, int) as select count(order_id), '' from order_detail where order_id >= $1 and order_id < $2 and order_id < $3;
-- explain analyze execute stmt(1, 10000, 2);
-- explain analyze execute stmt(1, 1000000, 1000000);
-- deallocate prepare stmt;


select count(*) from "order" as o inner join order_detail as od on od.order_id = o.id;

explain analyze select count(*) from "order" as o inner join order_detail as od on od.order_id = o.id;
explain analyze select count(*) from "order" as o inner join (select order_id from order_detail group by order_id) as od on od.order_id = o.id;

explain analyze select count(*) from client as c inner join client_ex as c_ex on c_ex.id = c.id;

select count(*) from "order" as o inner join order_detail as od on od.order_id = o.id;

/*
SET enable_hashjoin = off;
SET enable_hashjoin = on;
SET max_parallel_workers_per_gather = 0;

SHOW max_parallel_workers_per_gather;
 */


