if db_id('q1') is not null
begin
    create database q1;
end
go

use q1;
go

if object_id('dbo.q1', 'u') is not null drop table dbo.q1;    
go

create table dbo.q1 (
    id int not null,
    data char(100) not null,
    status_id int not null
);
go

with id as (
    select row_number() over (order by (select 1)) as id
    from sys.all_columns as t1
    cross join sys.all_columns as t2
)
insert into dbo.q1 (id, data, status_id)
select top (10000000)
    id.id,
    replicate('a', 100),
    case when id.id % 10 = 0 then 0 else 1 end as status_id
from id;
go
