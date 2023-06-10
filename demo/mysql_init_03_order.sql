use test_db;

drop table if exists `order`;

create table `order` (
    id int not null,
    dt datetime not null,
    client_id int not null,
    pay_type int not null,
    group_id int not null,

    primary key (id)
);


insert into `order`(id, dt, client_id, pay_type, group_id)
with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    date_add('2020-01-01', interval id second) as dt,
    floor(rand() * 10000) as client_id,
    floor(rand() * 5) as pay_type,
    floor(rand() * 1000) as group_id
from tmp
where id < 100000;

create index idx_product_client_id on `order`(client_id);
create index idx_order_pay_type on `order`(pay_type);
create index idx_order_group_id on `order`(group_id);

-- select count(*) from `order`;

