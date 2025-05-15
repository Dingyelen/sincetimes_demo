###
create table if not exists hive.demo_global_w.dws_user_hourly_hi
(date date,
hour timestamp,
role_id varchar,
zone_id varchar,
channel varchar,
money decimal(36, 2), 
app_money decimal(36, 2), 
web_money decimal(36, 2), 
pay_count bigint,
app_count bigint,
web_count bigint,
events array(varchar),
last_event varchar,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.dws_user_hourly_hi 
where part_date >= $start_date
and part_date <= $end_date;

insert into  hive.demo_global_w.dws_user_hourly_hi
(date, hour, role_id, zone_id, channel, 
money, app_money, web_money, 
pay_count, app_count, web_count, 
events, last_event, part_date)

with base_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, device_id, 
channel, zone_id, alliance_id, app_id, 
vip_level, level, rank_level, power, 
pay_source, payment_itemid, currency, money, online_time
from hive.demo_global_r.dwd_merge_base_live
where part_date >= $start_date
and part_date <= $end_date
), 

daily_gserver_info as(
select part_date, 
date(part_date) as date, 
date_trunc('hour', event_time) as hour,
role_id, app_id, 
zone_id, channel,
sum(money) as money, 
sum(case when event_name = 'Payment' and pay_source = 'app' then money else null end) as app_money, 
sum(case when event_name = 'Payment' and pay_source = 'web' then money else null end) as web_money, 
sum(case when event_name = 'Payment' then 1 else null end) as pay_count,
sum(case when event_name = 'Payment' and pay_source = 'app' then 1 else null end) as app_count, 
sum(case when event_name = 'Payment' and pay_source = 'web' then 1 else null end) as web_count, 
array_agg(event_name order by event_time) as events,
element_at(array_agg(event_name order by event_time), -1) as last_event
from base_log
group by 1, 2, 3, 4, 5, 6, 7
)

select date, hour, role_id, zone_id, channel, 
money, app_money, web_money, 
pay_count, app_count, web_count, 
events, last_event, part_date
from daily_gserver_info;
###
