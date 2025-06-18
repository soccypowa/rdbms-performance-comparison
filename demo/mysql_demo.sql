-- 01 - nonclustered index seek vs. scan
explain analyze select min(name) from client where country = 'UK'; -- 1
explain analyze select min(name) from client where country = 'NL'; -- 9
explain analyze select min(name) from client where country = 'FR'; -- 90
explain analyze select min(name) from client where country = 'CY'; -- 900
explain analyze select min(name) from client where country = 'US'; -- 4000
explain analyze select min(name) from client where country >= 'US'; -- 7333, table scan

explain analyze select min(name) from client where country >= 'US' and country < 'UT'; -- 4000, table scan
explain analyze select min(name) from client where country in ('US', 'UK'); -- 7334, table scan
explain analyze select min(name) from client where country in ('US', 'XX'); -- 7333, table scan
explain analyze select min(name) from client where country in ('UK', 'NL'); -- 10, index range scan

explain analyze select min(name) from client_large where country = 'UK'; -- 100, index lookup
explain analyze select min(name) from client_large where country = 'NL'; -- 900
explain analyze select min(name) from client_large where country = 'FR'; -- 9,000
explain analyze select min(name) from client_large where country = 'CY'; -- 90,000
explain analyze select min(name) from client_large where country = 'US'; -- 400,000
explain analyze select min(name) from client_large where country >= 'US'; -- 733,333, table scan


-- 02 - primary key lookup
select count(*) from client;
select count(*) from client_large;

explain analyze select id from client where id = 5000;
explain analyze select id from client_large where id = 500000;

explain analyze select name from client where id = 5000;
explain analyze select name from client_large where id = 500000;


-- 03 - clustered index range
explain analyze select min(name) from client where id >= 3000 and id < 5000;
explain analyze select min(name) from client_large where id >= 300000 and id < 500000;


-- 04 - table scan
-- 10%
explain analyze select count(*) from filter_1m where status_id_tinyint = 0;
explain analyze select count(*) from filter_1m where status_id_int = 0;
explain analyze select count(*) from filter_1m where status_char = 'deleted';
explain analyze select count(*) from filter_1m where status_varchar = 'deleted';
explain analyze select count(*) from filter_1m where status_text = 'deleted';

-- 90%
explain analyze select count(*) from filter_1m where status_id_tinyint = 1;
explain analyze select count(*) from filter_1m where status_id_int = 1;
explain analyze select count(*) from filter_1m where status_char = 'active';
explain analyze select count(*) from filter_1m where status_varchar = 'active';
explain analyze select count(*) from filter_1m where status_text = 'active';


-- 05 - select distinct / count distinct
explain analyze select count(distinct a) as cnt from group_by_table;
explain analyze select count(distinct b) as cnt from group_by_table;
explain analyze select count(distinct c) as cnt from group_by_table;

explain analyze select count(*) as cnt from (select a from group_by_table group by a) as tmp;
explain analyze select count(*) as cnt from (select b from group_by_table group by b) as tmp;
explain analyze select count(*) as cnt from (select c from group_by_table group by c) as tmp;


-- 06 - skip scan 1
explain analyze select c1, min(c2) as min_c2 from large_group_by_table group by c1;


-- 07 - skip scan 2
explain analyze select count(*) from skip_scan_example where b = 0;


-- 08 - index seek with complex condition
explain analyze select count(*) from client where id >= 1 and id < 10000 and id < 2;
explain analyze select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;
explain analyze select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;
explain analyze select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;

-- https://dev.mysql.com/doc/refman/8.0/en/range-optimization.html


-- 09 - join and aggregate 2 sorted tables
explain analyze select o.id as order_id, sum(od.price) as total_price from `order` as o inner join order_detail as od on od.order_id = o.id group by o.id;

-- pre-agg
explain analyze select o.id as order_id, sum(od_agg.price) as total_price from `order` as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id;

-- force hash join
# explain analyze select o.id as order_id, sum(od.price) as total_price from `order` as o inner join order_detail as od ignore index (primary) on od.order_id = o.id group by o.id;
explain analyze select o.id as order_id, sum(od.price) as total_price from `order` as o ignore index (primary) inner join order_detail as od ignore index (primary) on od.order_id = o.id group by o.id;


-- 10 - grouping with partial aggregation
explain analyze
    select p.name, count(*)
    from `order` as o
    inner join group_by_table as l on l.id = o.id
    inner join product as p on p.id = l.c
    group by p.name;

explain analyze
    select p.name, count(*)
    from `order` as o
    inner join group_by_table as l on l.id = o.id
    inner join product as p on p.id = l.a
    group by p.name;


-- 11 - combine select from 2 indexes
explain analyze select count(*)
from large_group_by_table as l
where l.c2 = 1 and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;

explain analyze select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;


-- 12 - dml
drop table if exists transactions;
drop table if exists transactions_modified;
drop table if exists transactions_wo_covered_index;

create table transactions (
    id int not null,
    description varchar(100) not null,
    ts timestamp not null,

    primary key (id)
);

create table transactions_modified (
                              id int not null,
                              description varchar(100) not null,
                              ts timestamp not null,

                              primary key (id)
);

create table transactions_wo_covered_index (
                                       id int not null,
                                       description varchar(100) not null,
                                       ts timestamp not null,

                                       primary key (id)
);

insert into transactions (id, description, ts)
with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    concat('desc_', id, '_', repeat('x', 50)) as description,
    date_add('2020-01-01 00:00:00', interval id second) as ts
from tmp
where id < 1000000;


create index ix_ts_description on transactions(ts, description); -- no include

insert into transactions_modified (id, description, ts)
with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    concat('desc_', id, '_', repeat('x', 50)) as description,
    date_add('2020-01-01 00:00:00', interval id second) as ts
from tmp
where id < 1000000;

create index ix_transactions_modified__ts_description on transactions_modified(ts, description);

insert into transactions_wo_covered_index (id, description, ts)
with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    concat('desc_', id, '_', repeat('x', 50)) as description,
    date_add('2020-01-01 00:00:00', interval id second) as ts
from tmp
where id < 1000000;

create index ix_transactions_wo_covered_index__ts on transactions_wo_covered_index(ts);

update transactions_modified set description = description;
update transactions_wo_covered_index set description = description;

explain analyze select ts, description from transactions where ts < '2020-01-01 01:00:00';
explain analyze select ts, description from transactions_modified where ts < '2020-01-01 01:00:00';
explain analyze select ts, description from transactions_wo_covered_index where ts < '2020-01-01 01:00:00';




















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








# explain analyze select count(*)
# from large_group_by_table as l
# where l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;









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

