use test_db;

drop table if exists numbers;

create table numbers (
    id int not null
);

insert into numbers(id)
with tmp as (
    select a.id + b.id * 10 + c.id * 100 + d.id * 1000 as id
    from (select 0 as id union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as a
             cross join (select 0 as id union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as b
             cross join (select 0 as id union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as c
             cross join (select 0 as id union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as d
)
select id from tmp;

-- select min(id), max(id), count(*), count(distinct id) from numbers;
