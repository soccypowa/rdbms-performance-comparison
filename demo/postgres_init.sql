-- client
drop table if exists client;
drop table if exists client_ex;

create table client (
    id int not null,
    name varchar(100) not null,
    country char(2) not null,
    insert_dt timestamp not null,

    primary key (id)
);

create table client_ex (
    id int not null,
    address varchar(1000) null,

    primary key (id)
);

insert into client(id, name, country, insert_dt)
select
    id,
    concat('client_', id),
    case
        when id % 100 = 0 then 'FR'
        when id % 10 = 0 then 'CY'
        when id % 2 = 0 then 'US'
        when id % 3 = 0 then 'DE'
        else 'UK'
        end,
    timestamp '2020-01-01' + id * interval '1 second'
from generate_series(1, 10000, 1) as numbers(id);

-- select country, count(*) from client group by country;

create index idx_client_country on client(country);

insert into client_ex(id, address)
select
    id,
    concat('client_', id, '_', repeat('x', 900))
from generate_series(1, 10000, 1) as numbers(id);



-- order
drop table if exists "order";

create table "order" (
    id int not null,
    dt timestamp not null,
    client_id int not null,
    pay_type int not null,
    group_id int not null,

    primary key (id)
);


insert into "order"(id, dt, client_id, pay_type, group_id)
select
    id,
    timestamp '2020-01-01' + id * interval '1 second' as dt,
    floor(random() * 10000) as client_id,
    floor(random() * 5) as pay_type,
    floor(random() * 1000) as group_id
from generate_series(1, 100000, 1) as numbers(id);

create index idx_product_client_id on "order"(client_id);
create index idx_order_pay_type on "order"(pay_type);
create index idx_order_group_id on "order"(group_id);


-- product
drop table if exists product;

create table product (
    id int not null,
    name varchar(100) not null,
    insert_dt timestamp not null,

    primary key (id)
);

insert into product(id, name, insert_dt)
select
    id,
    concat('product_', id),
    timestamp '2020-01-01' + id * interval '1 second' as dt
from generate_series(1, 1000000, 1) as numbers(id);


-- order_detail
drop table if exists order_detail;

create table order_detail (
    order_id int not null,
    product_id int not null,
    quantity int not null,
    price decimal(10,2) not null,

    primary key (order_id, product_id)
);

with tmp as (
    select
        o.id as id
    from "order" as o
    cross join generate_series(1, 10, 1) as numbers(id)
)
insert into order_detail(order_id, product_id, quantity, price)
select
    id,
    floor(random() * 1000000) as product_id,
    floor(random() * 10) + 1 as quantity,
    floor(random() * 100) as price
from tmp
on conflict (order_id, product_id) do nothing;

create index idx_order_detail_product on order_detail(product_id);


-- filter_10m
drop table if exists filter_10m;

create table filter_10m (
    id int not null,
    data char(100) not null,
    status_id_tinyint smallint not null,
    status_id_int int not null,
    status_char char(7) not null,
    status_varchar varchar(7) not null,
    status text not null
);

insert into filter_10m (id, data, status_id_tinyint, status_id_int, status_char, status_varchar, status)
select
    id,
    repeat('a', 100) as data,
    case when id % 10 = 0 then 0 else 1 end as status_id_tinyint,
    case when id % 10 = 0 then 0 else 1 end as status_id_int,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_char,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_varchar,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_text
from generate_series(1, 10000000, 1) as numbers(id);
