insert q4(id, data, status)
with id(id) as (
    select
        ROW_NUMBER() over (order by (select 1)) as id
    from information_schema.`COLUMNS` as t1
    cross join information_schema.`COLUMNS` as t2
    limit 10000000
)
select
    id.id,
    repeat('a', 100),
    case when id.id % 10 = 0 then 'deleted' else 'active' end
from id;