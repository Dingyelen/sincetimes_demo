create table if not exists hive.demo_global_w.dws_summon_daily_di(
date date, 
start_date date, 
zone_id varchar, 
summon_free bigint, 
summon_valid bigint, 
summon_count bigint, 
summon_continue bigint, 
summon_users bigint, 
core_cost bigint, 
retention_day bigint, 
summon_id varchar, 
part_date varchar
)
with(
format = 'ORC',
transactional = true,
partitioned_by = array['part_date']
);

delete from hive.demo_global_w.dws_summon_daily_di
where summon_id in (
select distinct cast(recruitid as varchar) as summon_id 
from hive.demo_global_r.dwd_gserver_recruitcard_live
where part_date >= $start_date
and part_date <= $end_date);

insert into  hive.demo_global_w.dws_summon_daily_di
(date, start_date, zone_id, 
summon_free, summon_valid, summon_count, summon_continue, summon_users, core_cost, 
retention_day, part_date, summon_id)
 
with summon_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
cast(recruitid as varchar) as summon_id, 
count as summon_count, 
free as is_free, costid as currency_id, costcount as currency_num
from hive.demo_global_r.dwd_gserver_recruitcard_live
where recruitid in (
select distinct recruitid
from hive.demo_global_r.dwd_gserver_recruitcard_live
where part_date >= $start_date
and part_date <= $end_date)
), 

summon_agg as(
select date, part_date, summon_id, zone_id, 
sum(case when is_free = 1 then summon_count else null end) as summon_free,
sum(case when is_free = 0 then summon_count else null end) as summon_valid, 
sum(summon_count) as summon_count, 
sum(case when summon_count = 10 then 10 else null end) as summon_continue, 
count(distinct role_id) as summon_users, 
sum(case when currency_id = 1 then currency_num else null end) as core_cost
from summon_log
group by 1, 2, 3, 4
), 

summon_rn as(
select date, part_date, summon_id, zone_id, 
summon_free, summon_valid, summon_count, summon_continue, summon_users, core_cost, 
row_number() over(partition by zone_id, summon_id order by date) as rn
from summon_agg
), 

summon_rn_cal as(
select date, part_date, summon_id, zone_id, 
summon_free, summon_valid, summon_count, summon_continue, summon_users, core_cost, 
rn, date_add('day', -rn + 1, date) as date_temp
from summon_rn 
), 

summon_retention as(
select date, part_date, summon_id, zone_id, 
summon_free, summon_valid, summon_count, summon_continue, summon_users, core_cost, 
rn, date_temp, 
row_number() over(partition by summon_id, zone_id, date_temp order by date) - 1 as retention_day, 
min(date) over(partition by summon_id, zone_id, date_temp order by date) as start_date
from summon_rn_cal
)

select date, start_date, zone_id, 
summon_free, summon_valid, summon_count, summon_continue, summon_users, core_cost, 
retention_day, part_date, summon_id
from summon_retention
;