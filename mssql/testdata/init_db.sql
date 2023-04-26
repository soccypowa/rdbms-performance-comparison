if db_id('test') is not null
begin
    drop database test;
end
go

create database test;
go
