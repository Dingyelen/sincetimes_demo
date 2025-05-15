create table if not exists hive.demo_global_w.ads_user_retention_di
(date date, 
install_date date,
zone_id varchar,
channel varchar,
os varchar, 
break_type varchar, 
retention_day bigint,
active_users bigint,
pay_users bigint,
newpay_users bigint,
money decimal(36, 2), 
money_rmb decimal(36, 2), 
online_time bigint,
payuser_ac bigint,
money_ac decimal(36, 2),
moneyrmb_ac decimal(36, 2),
new_users bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.ads_user_retention_di
where part_date >= '{yesterday}'
and part_date <= '{today}';

insert into hive.demo_global_w.ads_user_retention_di
(date, install_date, zone_id, channel, os, break_type, retention_day, 
active_users, pay_users, newpay_users, money, money_rmb, online_time, 
payuser_ac, money_ac, moneyrmb_ac, 
new_users, part_date)

with user_daily as(
select date, part_date, role_id, 
level_min, level_max, viplevel_min, viplevel_max, 
currency, money, online_time
from hive.demo_global_w.dws_user_daily_di 
), 

user_daily_join as(
select a.date, a.part_date, a.role_id, 
a.level_min, a.level_max, a.viplevel_min, a.viplevel_max,
a.money, a.money * z.rate as money_rmb, a.online_time, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.os, 
(case when b.install_date=b.firstpay_date then 'firstdate_break' 
when b.firstpay_date is not null then 'other_break'
else 'not_break' end) as break_type,  
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.demo_global_w.dws_user_info_di b
on a.role_id = b.role_id
left join mysql_bi_r."gbsp-bi-bigdata".t_currency_rate z
on a.currency = z.currency and date_format(a.date, '%Y-%m') = z.currency_time 
where b.is_test is null
and b.install_date >= date('{yesterday}')
and b.install_date <= date('{today}')
),

retention_info as(
select date, install_date, zone_id, channel, os, break_type, retention_day,
count(distinct role_id) as active_users,
count(distinct (case when money > 0 then role_id else null end)) as pay_users,
count(distinct (case when money > 0 and pay_retention_day = 0 then role_id else null end)) as newpay_users,
sum(money) as money, 
sum(money_rmb) as money_rmb, 
sum(online_time) as online_time
from user_daily_join
group by 1, 2, 3, 4, 5, 6, 7
),

retention_all as(
select install_date, zone_id, channel, os, break_type, 
sum(case when retention_day = 0 then active_users else null end) as new_users
from retention_info
group by 1, 2, 3, 4, 5
),

data_cube as(
select distinct install_date, zone_id, channel, os, break_type, t.retention_day
from retention_info
cross join unnest(sequence(0, 30, 1)) as t(retention_day)
),

retenion_info_cube as(
select date_add('day', a.retention_day, a.install_date) as date, 
a.install_date, a.zone_id, a.channel, a.os, a.break_type, a.retention_day,
b.active_users, b.pay_users, b.newpay_users, b.money, b.money_rmb, b.online_time, 
c.new_users
from data_cube a
left join retention_info b
on a.install_date = b.install_date 
and a.zone_id = b.zone_id 
and a.channel = b.channel 
and a.os = b.os 
and a.break_type = b.break_type
and a.retention_day = b.retention_day
left join retention_all c
on a.install_date = c.install_date
and a.zone_id = c.zone_id 
and a.channel = c.channel 
and a.os = c.os 
and a.break_type = c.break_type
)

select date, install_date, zone_id, channel, os, break_type, retention_day, 
active_users, pay_users, newpay_users, money, money_rmb, online_time, 
sum(newpay_users) over (partition by install_date, zone_id, channel, break_type, os order by retention_day
rows between unbounded preceding and current row) as payuser_ac,
sum(money) over (partition by install_date, zone_id, channel, break_type, os order by retention_day
rows between unbounded preceding and current row) as money_ac,
sum(money_rmb) over (partition by install_date, zone_id, channel, break_type, os order by retention_day
rows between unbounded preceding and current row) as moneyrmb_ac, 
new_users, date_format(install_date, '%Y-%m-%d') as part_date
from retenion_info_cube
;