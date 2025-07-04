use test_db;

drop table if exists group_by_table;

create table group_by_table (
    id int not null,
    a int not null,
    b int not null,
    c int not null,

    primary key (id)
);

insert into group_by_table(id, a, b, c)
with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    floor(rand() * 100000) as a,
    floor(rand() * 1000) as b,
    floor(rand() * 10) as c
from tmp
where id < 1000000;

create index idx_group_by_table_a on group_by_table(a);

create index idx_group_by_table_b on group_by_table(b);

create index idx_group_by_table_c on group_by_table(c);


drop table if exists skip_scan_example;

create table skip_scan_example (
    id int not null,
    a int not null,
    b int not null,
    c int not null,

    primary key (id)
);

insert into skip_scan_example(id, a, b, c)
with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    floor(rand() * 10) as a,
    floor(rand() * 1000) as b,
    floor(rand() * 100000) as c
from tmp
where id < 1000000;

# drop index idx_skip_scan_example_b_c on skip_scan_example;
create index idx_skip_scan_example_a_b on skip_scan_example(a, b);
