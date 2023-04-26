drop table if exists test_table;

create table test_table (
    id int not null,
    data char(100) not null,
    status char(7) not null
);

insert into test_table (id, data, status)
select
    id.id,
    repeat('a', 100),
    case when id.id % 10 = 0 then 'deleted' else 'active' end as status_id
from generate_series(1, 10000000, 1) as id(id);
