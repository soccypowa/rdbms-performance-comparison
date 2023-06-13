use test_db;
go

set showplan_text off;
set showplan_xml off;
set statistics profile off;

set showplan_text on;
set statistics profile on;
set statistics io on;
set statistics time on;


-- 01 - lookup by primary key
select id from client where id = 0;
select id from client where id = 9999;
select id from client where id = 100000;

select count(*) from order_detail as od where order_id = 1;

select order_id, count(*) from order_detail group by order_id
order by 2 desc

-- 02 - lookup by primary key + column not in index
select id, name from client where id = 1;
select id, name from client where id = 100000;

-- 03 - min and max
select min(id) from client;
select max(id) from client;
select min(id) + max(id) from client;


-- 04 - index seek with complex condition
select id, name from client where id >= 1 and id < 10000 and id > 9990;
select count(*), '' from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;

declare @a int = 1, @b int = 10000, @c int = 2;
select count(*), '' from order_detail where order_id >= @a and order_id < @b and order_id < @c;



-- create table dbo.seek_predicate_example (
--                                             dt datetime not null,
--                                             some_data char(1000) null
-- );
--
-- insert into dbo.seek_predicate_example (dt)
-- select top (100000)
--     dateadd(second, cast(floor(rand(checksum(newid())) * 3600 * 24 * 10) as int), '20160101')
-- from sys.all_columns as t1
--          cross join sys.all_columns as t2;
--
-- create clustered index ix_cl_seek_predicate_example on dbo.seek_predicate_example (dt);