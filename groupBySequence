#drop table accounts;
#drop table activities;

#create table if not EXISTS accounts(id integer PRIMARY KEY AUTO_INCREMENT, mac varchar(25));

#insert into accounts(mac) values ('m1');
#insert into accounts(mac) values ('m2');
#insert into accounts(mac) values ('m3');
#insert into accounts(mac) values ('m4');

#select * from accounts;

#create table if not exists activities(id integer PRIMARY KEY AUTO_INCREMENT, dt varchar(25), account_id integer NOT NULL, type varchar(10))

#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:01', 1, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:02', 1, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:03', 1, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:04', 1, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:05', 1, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:06', 1, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:07', 1, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:08', 1, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:09', 1, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:10', 1, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:11', 1, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:12', 1, 'ERROR');

#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:01', 2, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:02', 2, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:03', 2, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:04', 2, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:05', 2, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:06', 2, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:07', 2, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:08', 2, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:09', 2, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:10', 2, 'ERROR');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:11', 2, 'SUCCESS');
#insert into activities(dt, account_id, type) values ('01-01-1970 00:00:12', 2, 'SUCCESS');



SET @group_id := 0;
with src as (
select 
  id, account_id, dt, type, 
  lag(type) over (partition by account_id order by dt) prev_type
from activities),
cte_gid as (select *,   
  case 
  	when type = prev_type then @group_id
    else @group_id:= @group_id + 1
  end as gid from src)
  
  select account_id, min(dt) as started_dt, max(dt) as ended_dt, count(*) as `activities`
  from cte_gid
  where type = 'ERROR'
  group by gid
  order by account_id, activities DESC;

