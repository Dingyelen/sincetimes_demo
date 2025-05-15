create table if not exists hive.demo_global_w.dws_core_snapshot_di(
date date, 
role_id varchar, 
coreadd_detail varchar, 
freeadd_detail varchar, 
paidadd_detail varchar, 
corecost_detail varchar, 
freecost_detail varchar, 
paidcost_detail varchar, 
core_end bigint, 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.dws_core_snapshot_di
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}';

insert into hive.demo_global_w.dws_core_snapshot_di(
date, role_id, 
coreadd_detail, freeadd_detail, paidadd_detail, 
corecost_detail, freecost_detail, paidcost_detail, 
core_end, part_date
)

with base_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, device_id, 
channel, zone_id, alliance_id, app_id, 
vip_level, level, rank_level, power, 
payment_itemid, currency, money, online_time
from hive.demo_global_r.dwd_merge_base_live
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}'
), 

core_log_base as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
reason, event_type, 
coalesce(free_num, 0) as free_num, coalesce(paid_num, 0) as paid_num, 
coalesce(free_end, 0) as free_end, coalesce(paid_end, 0) as paid_end
from hive.demo_global_r.dwd_gserver_corechange_live
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}'
), 

core_log as(
select part_date, event_name, event_time, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, reason, 
(case when event_type = 'gain' then free_num else null end) as free_add, 
(case when event_type = 'gain' then paid_num else null end) as paid_add, 
(case when event_type = 'gain' then free_num + paid_num else null end) as core_add, 
(case when event_type = 'cost' then free_num else null end) as free_cost, 
(case when event_type = 'cost' then paid_num else null end) as paid_cost, 
(case when event_type = 'cost' then free_num + paid_num else null end) as core_cost, 
free_end, paid_end, free_end + paid_end as core_end
from core_log_base
), 

core_cal_log as(
select part_date, event_name, 
role_id, reason, 
sum(free_add) as free_add,
sum(paid_add) as paid_add,
sum(core_add) as core_add,
sum(free_cost) as free_cost,
sum(paid_cost) as paid_cost,
sum(core_cost) as core_cost
from core_log
group by 1, 2, 3, 4
), 

daily_gserver_info as(
select part_date, date, role_id
from base_log
group by 1, 2, 3
), 

daily_turn_array as(
select part_date, role_id, 
json_format(cast(map_agg(reason, core_add) filter (where core_add > 0) as json)) as coreadd_detail, 
json_format(cast(map_agg(reason, free_add) filter (where free_add > 0) as json)) as freeadd_detail, 
json_format(cast(map_agg(reason, paid_add) filter (where paid_add > 0) as json)) as paidadd_detail, 
json_format(cast(map_agg(reason, core_cost) filter (where core_cost > 0) as json)) as corecost_detail, 
json_format(cast(map_agg(reason, free_cost) filter (where free_cost > 0) as json)) as freecost_detail, 
json_format(cast(map_agg(reason, paid_cost) filter (where paid_cost > 0) as json)) as paidcost_detail
from core_cal_log
group by 1, 2
), 

daily_core_last as
(select distinct part_date, role_id,
last_value(core_end) ignore nulls over (partition by part_date, role_id order by event_time, core_end
rows between unbounded preceding and unbounded following) as core_end
from core_log
)

select a.date, a.role_id, 
b.coreadd_detail, 
b.freeadd_detail, 
b.paidadd_detail, 
b.corecost_detail, 
b.freecost_detail, 
b.paidcost_detail, 
c.core_end, 
a.part_date
from daily_gserver_info a
left join daily_turn_array b
on a.part_date = b.part_date
and a.role_id = b.role_id
left join daily_core_last c
on a.part_date = c.part_date
and a.role_id = c.role_id;