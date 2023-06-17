use test_db;

drop table if exists `filter_1m`;
drop table if exists `filter_1m_with_pk`;

create table filter_1m (
    id int not null,
    data char(100) not null,
    status_id_tinyint tinyint not null,
    status_id_int int not null,
    status_char char(7) not null,
    status_varchar varchar(7) not null,
    status_text longtext not null
);

create table filter_1m_with_pk (
    id int not null,
    data char(100) not null,
    status_id_tinyint tinyint not null,
    status_id_int int not null,
    status_char char(7) not null,
    status_varchar varchar(7) not null,
    status_text longtext not null,

    primary key (id)
);

insert into filter_1m (id, data, status_id_tinyint, status_id_int, status_char, status_varchar, status_text)
with tmp as (
    select
        a.id + b.id * 10000 as id
    from numbers as a
    cross join numbers as b
)
select
    id,
    repeat('a', 100) as data,
    case when id % 10 = 0 then 0 else 1 end as status_id_tinyint,
    case when id % 10 = 0 then 0 else 1 end as status_id_int,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_char,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_varchar,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_text
from tmp
where id < 1000000;

insert into filter_1m_with_pk (id, data, status_id_tinyint, status_id_int, status_char, status_varchar, status_text)
with tmp as (
    select
            a.id + b.id * 10000 as id
    from numbers as a
             cross join numbers as b
)
select
    id,
    repeat('a', 100) as data,
    case when id % 10 = 0 then 0 else 1 end as status_id_tinyint,
    case when id % 10 = 0 then 0 else 1 end as status_id_int,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_char,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_varchar,
    case when id % 10 = 0 then 'deleted' else 'active' end as status_text
from tmp
where id < 1000000;
