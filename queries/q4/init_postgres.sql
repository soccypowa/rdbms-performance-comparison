drop table if exists q4.public.q4;

create table q4.public.q4 (
    id int not null,
    data char(100) not null,
    status text not null
);

insert into q4.public.q4 (id, data, status)
select
    id.id,
    repeat('a', 100),
    case when id.id % 10 = 0 then 'deleted' else 'active' end as status_id
from generate_series(1, 10000000, 1) as id(id);