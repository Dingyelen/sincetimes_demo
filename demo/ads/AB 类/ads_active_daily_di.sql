create table if not exists hive.demo_global_w.ads_active_daily_di
(date date,
zone_id varchar, 
channel varchar, 
os varchar, 
level bigint, 
vip_level bigint, 
dau bigint, 
last7_dau bigint, 
last30_dau bigint, 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.ads_active_daily_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.demo_global_w.ads_active_daily_di
(date, zone_id, channel, os, level, vip_level, 
dau, last7_dau, last30_dau, 
part_date)

with user_daily as(
select date, part_date, role_id, 
level_min, level_max, viplevel_min, viplevel_max, money
from hive.demo_global_w.dws_user_daily_di 
where part_date >= date_format(date_add('day', -31, date($start_date)), '%Y-%m-%d')
and part_date <= $end_date
), 

user_daily_join as
(select 
a.date, a.part_date,
a.role_id, 
a.level_min, a.level_max, b.level, 
a.viplevel_min, a.viplevel_max, b.vip_level, a.money, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.os, 
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.demo_global_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
), 

date_cube as(
select distinct date, part_date
from user_daily_join
), 

date_cube_agg as(
select a.date, a.part_date, b.zone_id, b.channel, b.os, b.level, b.vip_level, 
count(distinct case when date_diff('day', b.date, a.date) between 1 and 7 then role_id else null end) as last7_dau, 
count(distinct case when date_diff('day', b.date, a.date) between 1 and 30 then role_id else null end) as last30_dau
from date_cube a
left join user_daily_join b
on a.date >= b.date
group by 1, 2, 3, 4, 5, 6, 7
), 

dau_info as(
select date, part_date, zone_id, channel, os, level, vip_level, 
count(distinct role_id) as dau
from user_daily_join
group by 1, 2, 3, 4, 5, 6, 7
)

select a.date, a.zone_id, a.channel, a.os, a.level, a.vip_level, 
dau, last7_dau, last30_dau, 
a.part_date
from date_cube_agg a
left join dau_info b
on a.date = b.date
and a.zone_id = b.zone_id
and a.channel = b.channel
and a.os = b.os
and a.level = b.level
and a.vip_level = b.vip_level
where a.date >= date($start_date)
and a.date <= date($end_date)
;