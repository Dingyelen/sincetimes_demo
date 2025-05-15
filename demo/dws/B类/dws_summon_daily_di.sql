create table if not exists hive.demo_global_w.dws_summon_daily_di(
date date, 
start_date date, 
zone_id varchar, 
summon_free bigint, 
summon_valid bigint, 
summon_num bigint, 
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
select distinct summon_id 
from hive.demo_global_r.dwd_gserver_summon_live
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}');

insert into  hive.demo_global_w.dws_summon_daily_di
(date, start_date, zone_id, 
summon_free, summon_valid, summon_num, summon_continue, summon_users, core_cost, 
retention_day, part_date, summon_id)
 
with summon_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
summon_id, summon_num, core_cost
from hive.demo_global_r.dwd_gserver_summon_live
where summon_id in (
select distinct summon_id
from hive.demo_global_r.dwd_gserver_summon_live
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}')
), 

summon_agg as(
select date, part_date, summon_id, zone_id, 
sum(case when core_cost = 0 then summon_num else null end) as summon_free,
sum(case when core_cost > 0 then summon_num else null end) as summon_valid, 
sum(summon_num) as summon_num, 
sum(case when summon_num = 10 then 10 else null end) as summon_continue, 
count(distinct role_id) as summon_users, 
sum(core_cost) as core_cost
from summon_log
group by 1, 2, 3, 4
), 

summon_rn as(
select date, part_date, summon_id, zone_id, 
summon_free, summon_valid, summon_num, summon_continue, summon_users, core_cost, 
row_number() over(partition by zone_id, summon_id order by date) as rn
from summon_agg
), 

summon_rn_cal as(
select date, part_date, summon_id, zone_id, 
summon_free, summon_valid, summon_num, summon_continue, summon_users, core_cost, 
rn, date_add('day', -rn + 1, date) as date_temp
from summon_rn 
), 

summon_retention as(
select date, part_date, summon_id, zone_id, 
summon_free, summon_valid, summon_num, summon_continue, summon_users, core_cost, 
rn, date_temp, 
row_number() over(partition by summon_id, zone_id, date_temp order by date) - 1 as retention_day, 
min(date) over(partition by summon_id, zone_id, date_temp order by date) as start_date
from summon_rn_cal
)

select date, start_date, zone_id, 
summon_free, summon_valid, summon_num, summon_continue, summon_users, core_cost, 
retention_day, part_date, summon_id
from summon_retention
;