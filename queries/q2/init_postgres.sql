drop database if exists q2;

create database q2;

drop table if exists q2.public.q2;

create table q2.public.q2 (
    id int not null,
    data char(100) not null,
    status char(7) not null
);

insert into q2.public.q2 (id, data, status)
select
    id.id,
    repeat('a', 100),
    case when id.id % 10 = 0 then 'deleted' else 'active' end as status_id
from generate_series(1, 10000000, 1) as id(id);
