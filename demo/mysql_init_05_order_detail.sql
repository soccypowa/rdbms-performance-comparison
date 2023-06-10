use test_db;

drop table if exists `order_detail`;

create table order_detail (
    order_id int not null,
    product_id int not null,
    quantity int not null,
    price decimal(10,2) not null,

    primary key (order_id, product_id)
);

insert ignore into order_detail(order_id, product_id, quantity, price)
with tmp as (
    select
        o.id as id
    from `order` as o
    cross join numbers as a
    where a.id < 10
)
select
    id,
    floor(rand() * 1000000) as product_id,
    floor(rand() * 10) + 1 as quantity,
    floor(rand() * 100) as price
from tmp;

create index idx_order_detail_product on order_detail(product_id);