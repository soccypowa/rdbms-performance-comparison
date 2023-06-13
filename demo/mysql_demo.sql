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
explain analyze select id, name from client where id >= 1 and id < 10000 and id < 2;
explain analyze select count(order_id), '' from order_detail where order_id >= 1 and order_id < 10000 and order_id < 2;

# set @sql = 'select count(order_id), '''' from order_detail where order_id >= ? and order_id < ? and order_id < ?';
# prepare stmt from @sql;
# set @a = 1;
# set @b = 10000;
# set @c = 2;
# execute stmt using @a, @b, @c;
# deallocate prepare stmt;

/*
 https://dev.mysql.com/doc/internals/en/prepared-stored-statement-execution.html

 That basically says that the execution plan created for the prepared statement at compile time is not used. At
 execution time, once the variables are bound, it uses the values to create a new execution plan and uses that one.
 */


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


explain analyze select * from client where country = 'FR';

explain analyze select country, id from client where country = 'FR';

select 'client' as tbl, count(*) as cnt from client
union all select 'product', count(*) from product
union all select 'order', count(*) from `order`
union all select 'order_detail', count(*) from `order_detail`;

explain analyze select *
from `order` as o
inner join order_detail as od on o.id = od.order_id
where o.id < 200;

explain analyze select *
from `order` as o
inner join order_detail as od on o.id = od.order_id
where od.order_id < 200;

explain analyze select * from `order` as o
where (o.client_id < 10000 or o.client_id > 90000) and o.group_id < 5;

select count(*) from `order` as o where o.client_id < 10000;
select count(*) from `order` as o where o.group_id < 10;

explain analyze select count(*)
from `order` as o
inner join order_detail od on o.id = od.order_id;

explain analyze select count(c.country), count(c_ex.address)
from client as c
inner join client_ex as c_ex on c_ex.id = c.id;
