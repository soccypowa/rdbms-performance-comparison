-- 01 - select distinct / count distinct
explain analyze select count(distinct a) as cnt from group_by_table;
explain analyze select count(distinct b) as cnt from group_by_table;
explain analyze select count(distinct c) as cnt from group_by_table;

explain analyze select count(*) as cnt from (select a from group_by_table group by a) as tmp;
explain analyze select count(*) as cnt from (select b from group_by_table group by b) as tmp;
explain analyze select count(*) as cnt from (select c from group_by_table group by c) as tmp;

show max_parallel_workers_per_gather;
set max_parallel_workers_per_gather = 1;
set max_parallel_workers_per_gather = 2;
set max_parallel_workers_per_gather = 4;

with recursive t as (
    select min(a) as x from group_by_table
    union all
    select (select min(a) from group_by_table where a > t.x) from t where t.x is not null
)
select count(*)
from (
    select x from t where x is not null
    union all
    select null where exists (select 1 from group_by_table where a is null)
) as tmp;

with recursive t as (
    select min(b) as x from group_by_table
    union all
    select (select min(b) from group_by_table where b > t.x) from t where t.x is not null
)
select count(*)
from (
    select x from t where x is not null
    union all
    select null where exists (select 1 from group_by_table where b is null)
) as tmp;

with recursive t as (
    select min(c) as x from group_by_table
    union all
    select (select min(c) from group_by_table where c > t.x) from t where t.x is not null
)
select count(*)
from (
    select x from t where x is not null
    union all
    select null where exists (select 1 from group_by_table where c is null)
) as tmp;


-- 01 - skip scan more complex  example
explain analyze select order_id, min(product_id) as min_product_id from order_detail group by order_id;
explain analyze select c1, min(c2) as min_c2 from large_group_by_table group by c1;


-- 02 - index seek with complex condition
explain analyze select count(*) from client where id >= 1 and id < 10000 and id < 2;
explain analyze select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;
explain analyze select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;
explain analyze select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;

-- prepare stmt(int, int, int) as select count(order_id), '' from order_detail where order_id >= $1 and order_id < $2 and order_id < $3;
-- explain analyze execute stmt(1, 10000, 2);
-- explain analyze execute stmt(1, 1000000, 1000000);
-- deallocate prepare stmt;


-- 02 - merge join
explain analyze select count(*) from client as c inner join client_ex as c_ex on c_ex.id = c.id;
explain analyze select count(*) from "order" as o inner join order_detail as od on od.order_id = o.id;

show enable_hashjoin;
set enable_hashjoin = off;
set enable_hashjoin = on;

/*
set max_parallel_workers_per_gather = 0;

show max_parallel_workers_per_gather;
*/

explain analyze select count(*) from "order" as o inner join (select order_id from order_detail group by order_id) as od on od.order_id = o.id;

explain analyze select count(*) from "order" as o inner join (select order_id, product_id from order_detail group by order_id, product_id) as od on od.order_id = o.id;

explain analyze select count(*) from client as a inner join client as b on a.name < b.name;


-- 03 - nonclustered index seek vs. scan
explain analyze select min(name) from client where country = 'UK'; -- 1, index scan
explain analyze select min(name) from client where country = 'NL'; -- 9, bitmap index scan
explain analyze select min(name) from client where country = 'FR'; -- 90
explain analyze select min(name) from client where country = 'CY'; -- 900
explain analyze select min(name) from client where country = 'US'; -- 4000
explain analyze select min(name) from client where country >= 'US' and country < 'UT'; -- 4000
explain analyze select min(name) from client where country >= 'US'; -- 7333, seq scan

explain analyze select min(name) from client where country >= 'US' and country < 'UT'; -- 4000, bitmap index scan
explain analyze select min(name) from client where country in ('US', 'UK'); -- 7334, bitmap index scan
explain analyze select min(name) from client where country in ('US', 'XX'); -- 7333, seq scan
explain analyze select min(name) from client where country in ('UK', 'NL'); -- 10, bitmap index scan

explain analyze select min(name) from client_large where country = 'UK'; -- 100, index scan
explain analyze select min(name) from client_large where country = 'NL'; -- 900, bitmap index scan
explain analyze select min(name) from client_large where country = 'FR'; -- 9,000
explain analyze select min(name) from client_large where country = 'CY'; -- 90,000
explain analyze select min(name) from client_large where country = 'US'; -- 400,000, parallel seq scan
explain analyze select min(name) from client_large where country >= 'US'; -- 733,333


-- 04 - join and aggregate 2 sorted tables
explain analyze select o.id as order_id, sum(od.price) as total_price from "order" as o inner join order_detail as od on od.order_id = o.id group by o.id;

-- pre-agg
explain analyze select o.id as order_id, sum(od_agg.price) as total_price from "order" as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id;

/*
set enable_hashjoin = off;
set enable_hashjoin = on;
set enable_mergejoin = off;
set enable_mergejoin = on;
set max_parallel_workers_per_gather = 0;
set max_parallel_workers_per_gather = 2;
set max_parallel_workers_per_gather = 4;
set work_mem = 4096;
set work_mem = 2048;
set work_mem = 1024;

show enable_mergejoin;
show enable_hashjoin;
show work_mem;
show max_parallel_workers_per_gather;
*/


-- 05 - grouping with partial aggregation
explain analyze
    select p.name, count(*)
    from "order" as o
    inner join group_by_table as l on l.id = o.id
    inner join product as p on p.id = l.c
    group by p.name;

explain analyze
    select p.name, count(*)
    from "order" as o
    inner join group_by_table as l on l.id = o.id
    inner join product as p on p.id = l.a
    group by p.name;

-- optimized
explain analyze
    select p.name, cnt
    from (
        select l.c, count(*) as cnt
        from "order" as o
        inner join group_by_table as l on l.id = o.id group by l.c
    ) as t
    inner join product as p on p.id = t.c;



-- 00 - table scan
explain analyze select count(*) from filter_1m where status_id_tinyint = 0;
explain analyze select count(*) from filter_1m where status_id_int = 0;
explain analyze select count(*) from filter_1m where status_char = 'deleted';
explain analyze select count(*) from filter_1m where status_varchar = 'deleted';
explain analyze select count(*) from filter_1m where status_text = 'deleted';

explain analyze select count(*) from filter_1m where status_id_tinyint = 1;
explain analyze select count(*) from filter_1m where status_id_int = 1;
explain analyze select count(*) from filter_1m where status_char = 'active';
explain analyze select count(*) from filter_1m where status_varchar = 'active';
explain analyze select count(*) from filter_1m where status_text = 'active';

explain analyze select count(*) from filter_1m;
explain analyze select count(*) from filter_1m_with_pk;
explain analyze select count(id) from filter_1m_with_pk;


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

-- 09 - combine select from 2 indexes
explain analyze select count(*)
from large_group_by_table as l
where l.c2 = 1 and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where l.c2 in (1, 2, 50) and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22) and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;
