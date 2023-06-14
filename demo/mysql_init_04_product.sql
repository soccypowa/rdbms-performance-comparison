use test_db;

drop table if exists `product`;

create table `product` (
    id int not null,
    name varchar(100) not null,
    insert_dt datetime not null,

    primary key (id)
);

insert into `product`(id, name, insert_dt)
with tmp as (
    select
            a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    concat('product_', id),
    date_add('2020-01-01', interval id second) as dt
from tmp
where id < 1000000;
