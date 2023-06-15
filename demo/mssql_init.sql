use master;
go

if db_id('test_db') is not null
    begin
        drop database test_db;
    end
go

create database test_db;
go

use test_db;
go

-- numbers
drop table if exists numbers;

create table numbers (
    id int not null,

    primary key clustered (id)
);

insert into numbers(id)
select top (10000) (row_number() over (order by (select 1))) - 1 as id
from sys.all_columns as a
cross join sys.all_columns as b;

-- client
drop table if exists client;
go
drop table if exists client_ex;
go

create table client (
    id int not null,
    name varchar(100) not null,
    country char(2) not null,
    insert_dt datetime not null,

    primary key clustered (id)
);
go

create table client_ex (
    id int not null,
    address varchar(1000) null,

    primary key clustered (id)
);
go

insert into client(id, name, country, insert_dt)
select
    id,
    concat('client_', id),
    case
        when id % 10000 = 0 then 'UK'
        when id % 1000 = 0 then 'NL'
        when id % 100 = 0 then 'FR'
        when id % 10 = 0 then 'CY'
        when id % 2 = 0 then 'US'
        when id % 3 = 0 then 'DE'
        else 'XX'
        end,
    dateadd(second, id, '2020-01-01')
from numbers;
go

create nonclustered index idx_client_country on client(country);
go

-- select country, count(*) from client group by country;

insert into client_ex(id, address)
select
    id,
    concat('client_', id, '_', replicate('x', 900))
from numbers;
go


-- order
drop table if exists [order];
go

create table [order] (
    id int not null,
    dt datetime not null,
    client_id int not null,
    pay_type int not null,
    group_id int not null,

    primary key clustered (id)
);
go

with tmp as (
    select
            a.id + b.id * 10000 as id
    from numbers as a
             cross join numbers as b
)
insert into [order](id, dt, client_id, pay_type, group_id)
select
    id,
    dateadd(second, id, '2020-01-01') as dt,
    floor(rand() * 10000) as client_id,
    floor(rand() * 5) as pay_type,
    floor(rand() * 1000) as group_id
from tmp
where id < 100000;
go

create index idx_product_client_id on [order](client_id);
go
create index idx_order_pay_type on [order](pay_type);
go
create index idx_order_group_id on [order](group_id);
go

-- select count(*) from `order`;


-- product
drop table if exists product;
go

create table product (
    id int not null,
    name varchar(100) not null,
    insert_dt datetime not null,

    primary key clustered (id)
);
go

with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
insert into product(id, name, insert_dt)
select
    id,
    concat('product_', id),
    dateadd(second, id, '2020-01-01') as dt
from tmp
where id < 1000000;
go


-- order_detail
drop table if exists order_detail;
go

create table order_detail (
    order_id int not null,
    product_id int not null,
    quantity int not null,
    price decimal(10,2) not null,

    primary key clustered (order_id, product_id) with (ignore_dup_key = on)
);
go

with tmp as (
    select
        o.id as id
    from [order] as o
    cross join numbers as a
    where a.id < 10
)
insert into order_detail(order_id, product_id, quantity, price)
select
    id,
    floor(rand(checksum(newid())) * 1000000) as product_id,
    floor(rand(checksum(newid())) * 10) + 1 as quantity,
    floor(rand(checksum(newid())) * 100) as price
from tmp;
go

create nonclustered index idx_order_detail_product on order_detail(product_id);
go


-- filter_10m
drop table if exists filter_10m;
go

create table filter_10m (
    id int not null,
    data char(100) not null,
    status_id_tinyint tinyint not null,
    status_id_int int not null,
    status_char char(7) not null,
    status_varchar varchar(7) not null,
    status_text varchar(max) not null
);
go

with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
insert into filter_10m (id, data, status_id_tinyint, status_id_int, status_char, status_varchar, status_text)
select
    id,
    replicate('a', 100) as data,
    case when id % 10 = 0 then 0 else 1 end as status_id_tinyint,
    case when id % 10 = 0 then 0 else 1 end as status_id_int,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_char,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_varchar,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_text
from tmp
where id < 10000000;
go


-- filter_1m
drop table if exists filter_1m;
go

create table filter_1m (
    id int not null,
    data char(100) not null,
    status_id_tinyint tinyint not null,
    status_id_int int not null,
    status_char char(7) not null,
    status_varchar varchar(7) not null,
    status_text varchar(max) not null
);
go

with tmp as (
    select
            a.id + b.id * 10000 as id
    from numbers as a
             cross join numbers as b
)
insert into filter_1m (id, data, status_id_tinyint, status_id_int, status_char, status_varchar, status_text)
select
    id,
    replicate('a', 100) as data,
    case when id % 10 = 0 then 0 else 1 end as status_id_tinyint,
    case when id % 10 = 0 then 0 else 1 end as status_id_int,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_char,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_varchar,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_text
from tmp
where id < 1000000;
go


-- large_group_by_table
drop table if exists large_group_by_table;

create table large_group_by_table (
    id int not null,
    c1 int not null,
    c2 int not null,
    c3 int not null,
    c4 int not null,
    data char(200) not null,

    primary key clustered (id)
);

with tmp as (
    select
            a.id + b.id * 10000 as id
    from numbers as a
             cross join numbers as b
)
insert into large_group_by_table(id, c1, c2, c3, c4, data)
select
    id,
    floor(rand(checksum(newid())) * 10) as c1,
    floor(rand(checksum(newid())) * 100) as c2,
    floor(rand(checksum(newid())) * 1000) as c3,
    floor(rand(checksum(newid())) * 1000000) as c4,
    replicate('x', 200) as data
from tmp
where id < 1000000;

create nonclustered index idx_large_group_by_table_c1_c2_c3_c4 on large_group_by_table(c1, c2, c3, c4);

create nonclustered index idx_large_group_by_table_c2 on large_group_by_table(c2);

create nonclustered index idx_large_group_by_table_c3 on large_group_by_table(c3);
