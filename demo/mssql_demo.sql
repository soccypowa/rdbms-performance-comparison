use test_db;
go

set showplan_text off;
set showplan_xml off;
set statistics profile off;

set showplan_text on;
set showplan_xml on;

-- Actual query plan + CPU and IO stats
set statistics profile on;
set statistics io on;
set statistics time on;


-- 01 - nonclustered index seek vs. scan
select count(*) from client;

select min(name) from client where country = 'UK'; -- 1, index seek + lookup
select min(name) from client where country = 'NL'; -- 9
select min(name) from client where country = 'FR'; -- 90, index scan
select min(name) from client where country = 'CY'; -- 900
select min(name) from client where country = 'US'; -- 4000
select min(name) from client where country >= 'US'; -- 7333

select count(*) from client_large;

select min(name) from client_large where country = 'UK'; -- 100, index seek + lookup
select min(name) from client_large where country = 'NL'; -- 900
select min(name) from client_large where country = 'FR'; -- 9,000, index scan
select min(name) from client_large where country = 'CY'; -- 90,000
select min(name) from client_large where country = 'US'; -- 400,000
select min(name) from client_large where country >= 'US'; -- 733,333


-- 02 - primary key lookup
select count(*) from client;
select count(*) from client_large;

select id from client where id = 5000;
select id from client_large where id = 500000;

select name from client where id = 5000;
select name from client_large where id = 500000;


-- 03 - clustered index range
select min(name) from client where id >= 3000 and id < 5000;
select min(name) from client_large where id >= 300000 and id < 500000;


-- 04 - table scan
-- 10%
select count(*) from filter_1m where status_id_tinyint = 0;
select count(*) from filter_1m where status_id_int = 0;
select count(*) from filter_1m where status_char = 'deleted';
select count(*) from filter_1m where status_varchar = 'deleted';
select count(*) from filter_1m where status_text = 'deleted';

-- 90%
select count(*) from filter_1m where status_id_tinyint = 1;
select count(*) from filter_1m where status_id_int = 1;
select count(*) from filter_1m where status_char = 'active';
select count(*) from filter_1m where status_varchar = 'active';
select count(*) from filter_1m where status_text = 'active';

-- 05 - select distinct / count distinct
select count(distinct a) as cnt from group_by_table;
select count(distinct b) as cnt from group_by_table;
select count(distinct c) as cnt from group_by_table;

select count(*) as cnt from (select a from group_by_table group by a) as tmp;
select count(*) as cnt from (select b from group_by_table group by b) as tmp;
select count(*) as cnt from (select c from group_by_table group by c) as tmp;

if object_id('tempdb..#result', 'U') is not null drop table #result;
create table #result (x int);
declare @current int;
select top (1) @current = a from group_by_table order by a;
while @@rowcount > 0
begin
    insert into #result values (@current);
    select top (1) @current = a from group_by_table where a > @current order by a;
end;
select count(*) from #result;

if object_id('tempdb..#result', 'U') is not null drop table #result;
create table #result (x int);
declare @current int;
select top (1) @current = b from group_by_table order by b;
while @@rowcount > 0
    begin
        insert into #result values (@current);
        select top (1) @current = b from group_by_table where b > @current order by b;
    end;
select count(*) from #result;

if object_id('tempdb..#result', 'U') is not null drop table #result;
create table #result (x int);
declare @current int;
select top (1) @current = c from group_by_table order by c;
while @@rowcount > 0
    begin
        insert into #result values (@current);
        select top (1) @current = c from group_by_table where c > @current order by c;
    end;
select count(*) from #result;

select count(*), min(id), max(id) from numbers;
select count(*), min(b), max(b) from group_by_table;

with min_max as (
    select min(b) as min_b, max(b) as max_b from group_by_table
),
possible_values as (
    select n.id
    from numbers as n
    inner join min_max as mm on n.id >= mm.min_b and n.id <= mm.max_b
),
result as (
    select pv.id
    from possible_values as pv
    where exists (select top (1) 1 from group_by_table as g where g.b = pv.id)
)
select count(*)
from result;

-- 06 - skip scan 1
select c1, min(c2) as min_c2 from large_group_by_table group by c1;

-- optimised
select min(t3.min_c2)
from (select distinct(c1) as c1 from large_group_by_table) as t
cross apply (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3;

--  super-optimised
select min(t4.min_c2)
from (select min(c1) as min_c1, max(c1) as max_c1 from large_group_by_table) as t
cross apply (select n.id from numbers as n where n.id >= t.min_c1 and n.id <= t.max_c1) as t2
cross apply (select min(t3.c2) as min_c2 from large_group_by_table as t3 where t3.c1 = t2.id) as t4;

-- super-super-optimised
select min(t3.min_c2)
from (select 0 as c1 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as t
cross apply (select min(t2.c2) as min_c2 from large_group_by_table as t2 where t2.c1 = t.c1) as t3;


-- 07 - skip scan 2
select count(*) from skip_scan_example where b = 0;


-- 08 - index seek with complex condition
select count(*) from client where id >= 1 and id < 10000 and id < 2;
select count(*) from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;
select count(*) from order_detail where order_id >= 1 and order_id < 100000 and order_id < 2;
select count(*) from order_detail where order_id >= 1 and order_id < 2 and order_id < 100000;


-- 09 - join and aggregate 2 sorted tables
select o.id as order_id, sum(od.price) as total_price from [order] as o inner join order_detail as od on od.order_id = o.id group by o.id;
select o.id as order_id, sum(od.price) as total_price from [order] as o inner join order_detail as od on od.order_id = o.id group by o.id option (maxdop 1);

-- merge join
select o.id as order_id, sum(od.price) as total_price from [order] as o inner merge join order_detail as od on od.order_id = o.id group by o.id;

-- pre-agg
select o.id as order_id, sum(od_agg.price) as total_price from [order] as o inner join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id;

-- pre-agg with merge join
select o.id as order_id, sum(od_agg.price) as total_price from [order] as o inner merge join (select od.order_id, sum(od.price) as price from order_detail as od group by od.order_id) as od_agg on od_agg.order_id = o.id group by o.id;

-- loop join
select o.id as order_id, sum(od.price) as total_price from [order] as o inner loop join order_detail as od on od.order_id = o.id group by o.id option (maxdop 1)


-- 10 - grouping with partial aggregation
-- small
select p.name, count(*) as cnt
from [order] as o
inner join group_by_table as l on l.id = o.id
inner join product as p on p.id = l.c
group by p.name;
-- big
select p.name, count(*) as cnt
from [order] as o
inner join group_by_table as l on l.id = o.id
inner join product as p on p.id = l.a
group by p.name;





-- declare @a int = 1, @b int = 10000, @c int = 2;
-- select count(*) from order_detail where order_id >= @a and order_id < @b and order_id < @c;










-- 06 - combine select from 2 indexes
select count(*)
from large_group_by_table as l
where l.c2 = 1 and l.c3 = 1;

select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;

select count(*)
from large_group_by_table as l
inner loop join large_group_by_table as l2 on l2.id = l.id
    and (l2.c2 = 1 or l2.c2 = 2 or l2.c2 = 50)
where l.c3 = 1;

select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;

-- explain analyze select count(*)
-- from large_group_by_table as l
-- where l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21) and l.c3 = 1;












select count(*) from order_detail as od where order_id = 1;


-- 02 - lookup by primary key + column not in index


-- 03 - min and max
select min(id) from client;
select max(id) from client;
select min(id) + max(id) from client;


-- 09 - combine select from 2 indexes
select count(*)
from large_group_by_table as l
where l.c2 = 1 and l.c3 = 1

select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 = 50) and l.c3 = 1;

select count(*)
from large_group_by_table as l
where l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
               31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45) and l.c3 = 1;

select count(*)
from large_group_by_table as l
where l.c2 in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
               31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46) and l.c3 = 1;

select count(*)
from large_group_by_table as l
where l.c2 >= 0 and l.c2 < 50 and l.c3 = 1;

select count(*)
from large_group_by_table as l
where (l.c2 = 1 or l.c2 = 2 or l.c2 > 50) and l.c3 = 1;


ALTER DATABASE test_db SET COMPATIBILITY_LEVEL = 140; -- 2017
ALTER DATABASE test_db SET COMPATIBILITY_LEVEL = 150; -- 2019
ALTER DATABASE test_db SET COMPATIBILITY_LEVEL = 160; -- 2022
