create table if not exists hive.demo_global_w.dws_hero_snapshot_di(
date date, 
role_id varchar, 
hero_id varchar, 
hero_level bigint,
hero_star bigint,
chip_cost bigint,
upgrade_count bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.dws_hero_snapshot_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.demo_global_w.dws_hero_snapshot_di(
date, role_id, hero_id, hero_level, hero_star, chip_cost, upgrade_count, part_date
)

with upgraderare_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
cast(hero as varchar) as hero_id, 
cast(substring(cast(fromrare as varchar), 3, 1) as bigint) as hero_star, 
costchip as chip_cost, remainchip as chip_end, newskill as new_skill
from hive.demo_global_r.dwd_gserver_upgraderare_live
where role_id in 
(select distinct role_id 
from hive.demo_global_r.dwd_gserver_upgraderare_live 
where part_date >= $start_date
and  part_date <= $end_date)
), 

upgraderare_cal as(
select date, part_date, role_id, hero_id, 
max(hero_star) as hero_star,
sum(chip_cost) as chip_cost, 
count(*) as upgrade_count
from upgraderare_log
group by 1, 2, 3, 4
),

upgradehero_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
cast(hero as varchar) as hero_id, 
fromlv as original_level, tolv as hero_level
from hive.demo_global_r.dwd_gserver_upgradehero_live
where role_id in 
(select distinct role_id 
from hive.demo_global_r.dwd_gserver_upgradehero_live 
where part_date >= $start_date
and part_date <= $end_date)
), 

upgradehero_cal as(
select date, part_date, role_id, hero_id, 
max(hero_level) as hero_level
from upgradehero_log
group by 1, 2, 3, 4
), 

cal_info as(
select 
coalesce(a.date, b.date) as date, 
coalesce(a.part_date, b.part_date) as part_date, 
coalesce(a.role_id, b.role_id) as role_id, 
coalesce(a.hero_id, b.hero_id) as hero_id, 
b.hero_level, a.hero_star, a.chip_cost, a.upgrade_count
from upgraderare_cal a
full join upgradehero_cal b
on a.part_date = b.part_date 
and a.role_id = b.role_id 
and a.hero_id = b.hero_id
), 

cal_fit as(
select date, part_date, role_id, hero_id, 
max(hero_level) over(partition by role_id, hero_id order by date rows between unbounded preceding and current row) as hero_level,
max(hero_star) over(partition by role_id, hero_id order by date rows between unbounded preceding and current row) as hero_star,
chip_cost, upgrade_count
from cal_info
)

select date, role_id, hero_id, hero_level, hero_star, chip_cost, upgrade_count, part_date
from cal_fit
where part_date >= $start_date
and part_date <= $end_date