-- 01 - select distinct / count distinct
explain analyze select count(distinct a) as cnt from group_by_table;
explain analyze select count(distinct b) as cnt from group_by_table;
explain analyze select count(distinct c) as cnt from group_by_table;

explain analyze select count(*) as cnt from (select a from group_by_table group by a) as tmp;
explain analyze select count(*) as cnt from (select b from group_by_table group by b) as tmp;
explain analyze select count(*) as cnt from (select c from group_by_table group by c) as tmp;


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

# select @@innodb_parallel_read_threads;
# set local innodb_parallel_read_threads=1;

-- https://www.percona.com/blog/mysql-8-0-14-a-road-to-parallel-query-execution-is-wide-open/

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
explain analyze select count(*) from client where id >= 1 and id < 10000 and id < 2;
explain analyze select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;

# set @sql = 'select count(order_id), '''' from order_detail where order_id >= ? and order_id < ? and order_id < ?';
# prepare stmt from @sql;
# set @a = 1;
# set @b = 10000;
# set @c = 2;
# execute stmt using @a, @b, @c;d
# deallocate prepare stmt;

/*
 https://dev.mysql.com/doc/internals/en/prepared-stored-statement-execution.html

 That basically says that the execution plan created for the prepared statement at compile time is not used. At
 execution time, once the variables are bound, it uses the values to create a new execution plan and uses that one.
 */


-- 05 - nonclustered index seek vs. scan
explain analyze select count(name) from client where country = 'UK'; -- 1
explain analyze select count(name) from client where country = 'NL'; -- 9
explain analyze select count(name) from client where country = 'FR'; -- 90
explain analyze select count(name) from client where country = 'CY'; -- 900
explain analyze select count(name) from client where country = 'US'; -- 4000
explain analyze select count(name) from client where country >= 'US'; -- 7333, seq scan

explain analyze select min(name) from client where country = 'UK'; -- 1
explain analyze select min(name) from client where country = 'NL'; -- 9
explain analyze select min(name) from client where country = 'FR'; -- 90
explain analyze select min(name) from client where country = 'CY'; -- 900
explain analyze select min(name) from client where country = 'US'; -- 4000
explain analyze select min(name) from client where country >= 'US'; -- 7333, seq scan

explain analyze select min(name) from client_large where country = 'UK'; -- 100, index lookup
explain analyze select min(name) from client_large where country = 'NL'; -- 900
explain analyze select min(name) from client_large where country = 'FR'; -- 9,000
explain analyze select min(name) from client_large where country = 'CY'; -- 90,000
explain analyze select min(name) from client_large where country = 'US'; -- 400,000
explain analyze select min(name) from client_large where country >= 'US'; -- 733,333, table scan


-- 06 - join 2 sorted tables
explain analyze select count(*) from client as c inner join client_ex as c_ex on c_ex.id = c.id;
explain analyze select count(*) from `order` as o inner join order_detail as od on od.order_id = o.id;

explain analyze select count(*) from `order` as o inner join (select order_id from order_detail group by order_id) as od on od.order_id = o.id;

explain analyze select /*+ BNL(o, od) */ count(*) from `order` as o inner join order_detail as od on od.order_id = o.id;

explain analyze select count(*) from client as a inner join client as b on a.name < b.name;

-- https://dev.mysql.com/doc/refman/8.0/en/optimizer-hints.html#optimizer-hints-table-level


-- 07 - grouping
explain analyze select min(min_product_id) from (select order_id, min(product_id) as min_product_id from order_detail group by order_id) as t;

-- Loose Index Scan in action
explain analyze select min(min_c2) from (select c1, min(c2) as min_c2 from large_group_by_table group by c1) as t;


-- 08 - grouping with partial aggregation
explain analyze select count(*)
from (
    select p.name, count(*)
    from `order` as o
    inner join large_group_by_table as l on l.id = o.id
    inner join product as p on p.id = l.c1
    group by p.name
) as t;

explain analyze select count(*)
from (
    select p.name, count(*)
    from `order` as o
    inner join large_group_by_table as l on l.id = o.id
    inner join product as p on p.id = l.c4
    group by p.name
) as t;


-- 09 - combine select from 2 indexes
explain analyze select count(*)
from large_group_by_table as l
where l.c2 = 1 and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;


/*
 A numeric value derived from bit-level information that identifies the index type.
 0 = nonunique secondary index;
 1 = automatically generated clustered index (GEN_CLUST_INDEX);
 2 = unique nonclustered index;
 3 = clustered index;
 32 = full-text index;
 64 = spatial index;
 128 = secondary index on a virtual generated column.
 */

select ii.index_id, ii.name, ii.table_id, ii.type, t.name
from information_schema.innodb_indexes ii
join information_schema.innodb_tables t on ii.table_id = t.table_id
where t.name = 'test_db/numbers';

select ii.index_id, ii.name, ii.table_id, ii.type, t.name
from information_schema.innodb_indexes ii
join information_schema.innodb_tables t on ii.table_id = t.table_id
where t.name = 'test_db/client';

select ii.index_id, ii.name, ii.table_id, ii.type, t.name
from information_schema.innodb_indexes ii
join information_schema.innodb_tables t on ii.table_id = t.table_id
where t.name = 'test_db/order';


-- Check test data
select 'client' as tbl, count(*) as cnt from client
union all select 'product', count(*) from product
union all select 'order', count(*) from `order`
union all select 'order_detail', count(*) from `order_detail`;

