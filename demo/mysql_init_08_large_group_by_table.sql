use test_db;

drop table if exists `large_group_by_table`;

create table large_group_by_table (
    id int not null,
    c1 int not null,
    c2 int not null,
    c3 int not null,
    c4 int not null,
    data char(200) not null,

    primary key (id)
);

insert into large_group_by_table(id, c1, c2, c3, c4, data)
with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    floor(rand() * 10) as c1,
    floor(rand() * 100) as c2,
    floor(rand() * 1000) as c3,
    floor(rand() * 1000000) as c4,
    repeat('x', 200) as data
from tmp
where id < 1000000;

create index idx_large_group_by_table_c1_c2_c3_c4 on large_group_by_table(c1, c2, c3, c4);

create index idx_large_group_by_table_c2 on large_group_by_table(c2);

create index idx_large_group_by_table_c3 on large_group_by_table(c3);
