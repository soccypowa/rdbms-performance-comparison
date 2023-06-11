-- 01 - lookup by primary key
explain analyze select id from client where id = 1;
explain analyze select id from client where id = 100000;

-- 02 - lookup by primary key + column not in index
explain analyze select id, name from client where id = 1;
explain analyze select id, name from client where id = 100000;


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


