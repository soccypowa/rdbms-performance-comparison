drop table if exists q1;

create table q1 (
    id int not null,
    data char(100) not null,
    status_id int not null
);

insert into public.q1 (id, data, status_id)
select
    id.id,
    repeat('a', 100),
    case when id.id % 10 = 0 then 0 else 1 end as status_id
from generate_series(1, 10000000, 1) as id(id);
