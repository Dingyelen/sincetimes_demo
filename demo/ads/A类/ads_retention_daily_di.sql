create table if not exists hive.demo_global_w.ads_retention_daily_di
(date date,
retention_day bigint, 
dau bigint, 
active_users bigint, 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.ads_retention_daily_di
where part_date >= date_format(date_add('day', -31, date('{yesterday}')), '%Y-%m-%d')
and part_date <= '{today}';

insert into hive.demo_global_w.ads_retention_daily_di
(date, retention_day, dau, active_users, part_date)

with user_daily as(
select date, part_date, role_id, 
level_min, level_max, viplevel_min, viplevel_max, money
from hive.demo_global_w.dws_user_daily_di 
where part_date >= date_format(date_add('day', -31, date('{yesterday}')), '%Y-%m-%d')
and part_date <= '{today}'
), 

user_daily_join as
(select 
a.date, a.part_date,
a.role_id, 
a.level_min, a.level_max,
a.viplevel_min, a.viplevel_max, a.money, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel,
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.demo_global_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test = 0
), 

data_cube as(
select distinct date, part_date, active_date, role_id, 
date_diff('day', date, active_date) as retention_day
from user_daily_join
cross join unnest(filter(sequence(date, date_add('day', 14, date), interval '1' day), x -> x <= current_date)) as t(active_date)
), 

data_cube_join as(
select a.date, a.part_date, a.active_date, a.role_id, a.retention_day, 
(case when b.role_id is not null then 1 else 0 end) as active_users
from data_cube a
left join user_daily_join b
on a.role_id = b.role_id and a.active_date = b.date
), 

dau_info as(
select date, part_date, count(distinct role_id) as dau
from user_daily_join
group by 1, 2
), 

active_res as(
select date, retention_day, 
sum(active_users) as active_users
from data_cube_join
group by 1, 2
)

select a.date, retention_day, dau, active_users, 
part_date 
from active_res a
left join dau_info b
on a.date = b.date