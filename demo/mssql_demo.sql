use test_db;
go

set showplan_text off;
set showplan_xml off;
set statistics profile off;

set showplan_text on;
set statistics profile on;


-- 01 - lookup by primary key
select id from client where id = 1;
select id from client where id = 100000;

-- 02 - lookup by primary key + column not in index
select id, name from client where id = 1;
select id, name from client where id = 100000;

-- 03 - min and max
select min(id) from client;
select max(id) from client;
select min(id), max(id) from client;


select count(*) from [order] as o inner join order_detail as od on od.order_id = o.id;

select count(*) from client as c inner join client_ex as c_ex on c_ex.id = c.id;
