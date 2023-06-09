drop table if exists numbers;

drop table if exists test_table;

create table test_table (
    id int not null,
    data char(100) not null,
    status_id tinyint not null
);

create table numbers (
    id int not null
);

select min(id), max(id), count(*), count(distinct id) from numbers;

insert into numbers(id)
with seq1000 as (
    select a.id + b.id * 10 + c.id * 100 + d.id * 1000 as id
    from (select 0 as id union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as a
    cross join (select 0 as id union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as b
    cross join (select 0 as id union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as c
    cross join (select 0 as id union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as d
)
select id from seq1000;

/*
with id as (
    select a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select min(id), max(id), count(*), count(distinct id) from id where id < 10000000;
-- */

insert into test_table (id, data, status_id)
with id as (
    select a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id.id,
    repeat('a', 100),
    case when id.id % 10 = 0 then 0 else 1 end as status_id
from id where id.id < 10000000;

select count(*) from test_table;
