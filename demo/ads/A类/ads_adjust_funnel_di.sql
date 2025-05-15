create table if not exists hive.demo_global_w.ads_adjust_funnel_di(
funnel_type varchar,
funnel_stype varchar,
install_date date,
network varchar,
country varchar,
os varchar,
event_name varchar,
step_num bigint,
union_step varchar,
agg_users bigint,
demension_users bigint,
users bigint,
normal_users bigint,
normal_timediff bigint,
abnormal_users bigint,
abnormal_timediff bigint,
timediff_50 double, 
timediff_75 double, 
timediffac_50 double, 
timediffac_75 double, 
part_date varchar)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.ads_adjust_funnel_di 
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}';

insert into hive.demo_global_w.ads_adjust_funnel_di(
funnel_type, funnel_stype, 
install_date, network, country, os, 
event_name, step_num, union_step, 
agg_users, demension_users, users, 
normal_users, normal_timediff, 
abnormal_users, abnormal_timediff, 
timediff_50, timediff_75, timediffac_50, timediffac_75, part_date)

with user_tag as(
select role_id, adid, is_test, 
install_ts, install_date, network, country, os
from hive.demo_global_w.dws_user_info_di
where install_date >= date_add('day', -6, date '{yesterday}')
and install_date <= date '{today}'
), 

adjust_log_select as(
select date(event_time) as date, part_date, event_time, 
a.role_id, a.adid, 
cast((from_unixtime(cast(installed_at as bigint), 'UTC')) as timestamp) as install_ts, a.installed_at, 
date(cast((from_unixtime(cast(installed_at as bigint), 'UTC')) as timestamp)) as install_date,
a.network_name as network, a.country, a.os_name as os, 
a.adjust_event_name as event_name, b.step_num, 
b.is_compulsory, b.union_step
from hive.demo_global_r.dwd_adjust_live a
left join hive.demo_global_w.dim_adjust_loading_adjusteventname b
on a.adjust_event_name = b.event_name
where date(cast((from_unixtime(cast(installed_at as bigint), 'UTC')) as timestamp)) >= date_add('day', -6, date '{yesterday}')
and date(cast((from_unixtime(cast(installed_at as bigint), 'UTC')) as timestamp)) <= date '{today}'
and b.is_compulsory = '1'
), 

adjust_first_tag as(
select distinct adid, 
first_value(role_id) ignore nulls over(partition by adid order by event_time rows between unbounded preceding and unbounded following) as roleid_first, 
last_value(role_id) ignore nulls over(partition by adid order by event_time rows between unbounded preceding and unbounded following) as roleid_last, 
first_value(network) ignore nulls over(partition by adid order by event_time rows between unbounded preceding and unbounded following) as network, 
first_value(country) ignore nulls over(partition by adid order by event_time rows between unbounded preceding and unbounded following) as country, 
first_value(os) ignore nulls over(partition by adid order by event_time rows between unbounded preceding and unbounded following) as os
from adjust_log_select
), 

adjust_agg_tag as(
select adid, 
listagg(role_id, ',') within group(order by event_time) as roleid_list, 
min(install_ts) as install_ts, 
min(install_date) as install_date
from adjust_log_select
group by 1
), 

adjust_log_total as(
select 'loading' as funnel_type, 'total' as funnel_stype, 
a.date, a.part_date, a.event_time, 
a.adid as target_roleid, b.roleid_first, b.roleid_last, c.roleid_list, 
c.install_ts, c.install_date, 
b.network, b.country, b.os, 
a.event_name, a.step_num, a.is_compulsory, a.union_step
from adjust_log_select a
left join adjust_first_tag b
on a.adid = b.adid
left join adjust_agg_tag c
on a.adid = c.adid
), 

adjust_log_24h as(
select 'loading' as funnel_type, '24h' as funnel_stype, 
date, part_date, event_time, 
target_roleid, roleid_first, roleid_last, roleid_list, 
install_ts, install_date, 
network, country, os, 
event_name, step_num, is_compulsory, union_step
from adjust_log_total
where event_time <= date_add('hour', 24, install_ts)
), 

guidestep_log_select as(
select date(event_time) as date, part_date, event_time, 
a.role_id, c.install_date, c.install_ts, 
c.network, c.country, c.os, 
a.step_id, b.step_num, b.is_compulsory, b.union_step
from hive.demo_global_r.dwd_gserver_guidestep_live a
left join hive.demo_global_w.dim_gserver_guidestep_stepid b
on a.step_id = b.step_id and a.group_id = b.group_id
left join user_tag c
on a.role_id = c.role_id
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and b.is_compulsory = '1'
and c.is_test = 0
), 

guidestep_log_total as(
select 'guide' as funnel_type, 'total' as funnel_stype, 
date, part_date, event_time, 
role_id as target_roleid, role_id as roleid_first, role_id as roleid_last, role_id as roleid_list, 
install_ts, install_date, 
network, country, os, 
step_id, step_num, is_compulsory, union_step
from guidestep_log_select
), 

guidestep_log_24h as(
select 'guide' as funnel_type, '24h' as funnel_stype, 
date, part_date, event_time, 
role_id as target_roleid, role_id as roleid_first, role_id as roleid_last, role_id as roleid_list, 
install_ts, install_date, 
network, country, os, 
step_id, step_num, is_compulsory, union_step
from guidestep_log_select
where event_time <= date_add('hour', 24, install_ts)
), 

adjust_log as(
select * from adjust_log_total
union all
select * from adjust_log_24h
union all
select * from guidestep_log_total
union all
select * from guidestep_log_24h
), 

adjust_info as(
select funnel_type, funnel_stype, 
target_roleid, roleid_first, roleid_last, roleid_list, 
install_ts, install_date, 
network, country, os, 
event_name, step_num, is_compulsory, union_step, 
min(event_time) as event_time
from adjust_log 
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
), 

adjust_rank as(
select funnel_type, funnel_stype, 
target_roleid, roleid_first, roleid_last, roleid_list, 
install_ts, install_date, 
network, country, os, 
event_name, step_num, is_compulsory, union_step, 
row_number() over(partition by target_roleid, funnel_type, funnel_stype order by step_num desc) as rn_desc, 
date_diff('second', event_time, lead(event_time, 1) over(partition by target_roleid, funnel_type, funnel_stype order by step_num)) as time_diff, 
lead(step_num, 1) over(partition by target_roleid, funnel_type, funnel_stype order by step_num) as next_step_num
from adjust_info 
), 

adjust_rank_ac as(
select funnel_type, funnel_stype, 
target_roleid, roleid_first, roleid_last, roleid_list, 
install_ts, install_date, 
network, country, os, 
event_name, step_num, is_compulsory, union_step, time_diff, 
sum(time_diff) over(partition by target_roleid, funnel_type, funnel_stype order by step_num) as timediff_ac, 
rn_desc, next_step_num
from adjust_rank
), 

adjust_last as(
select funnel_type, funnel_stype, 
install_date, network, country, os, 
event_name, step_num, is_compulsory, union_step, 
count(distinct target_roleid) as demension_users
from adjust_rank_ac
where rn_desc = 1
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), 

adjust_rank_cal as(
select funnel_type, funnel_stype, 
install_date, network, country, os, 
event_name, step_num, is_compulsory, union_step, 
count(distinct case 
when funnel_type = 'loading' and time_diff <= 60 and time_diff >= 0 then target_roleid
when funnel_type = 'guide' and time_diff <= 600 and time_diff >= 0 then target_roleid else null end) as normal_users, 
sum(case 
when funnel_type = 'loading' and time_diff <= 60 and time_diff >= 0 then time_diff
when funnel_type = 'guide' and time_diff <= 600 and time_diff >= 0 then time_diff else null end) as normal_timediff, 
count(distinct case 
when funnel_type = 'loading' and (time_diff > 60 or time_diff < 0) then target_roleid
when funnel_type ='guide' and (time_diff > 600 or time_diff < 0) then target_roleid else null end) as abnormal_users, 
sum(case 
when funnel_type = 'loading' and (time_diff > 60 or time_diff < 0) then time_diff
when funnel_type ='guide' and (time_diff > 600 or time_diff < 0) then time_diff else null end) as abnormal_timediff, 
approx_percentile(time_diff, 0.5) as timediff_50, 
approx_percentile(time_diff, 0.75) as timediff_75
from adjust_rank_ac
where next_step_num = step_num + 1
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), 

adjust_rank_percal as(
select funnel_type, funnel_stype, 
install_date, network, country, os, 
event_name, step_num, is_compulsory, union_step, 
approx_percentile(timediff_ac, 0.5) as timediffac_50, 
approx_percentile(timediff_ac, 0.75) as timediffac_75 
from adjust_rank_ac
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), 

adjust_demension_agg as(
select funnel_type, funnel_stype, 
install_date, network, country, os, 
count(distinct target_roleid) as agg_users
from adjust_info
group by 1, 2, 3, 4, 5, 6
), 

data_cube_adjust as(
select funnel_type, funnel_stype, install_date, network, country, os, event_name, step_num, union_step
from 
(select distinct event_name, step_num, union_step from hive.demo_global_w.dim_adjust_loading_adjusteventname where is_compulsory = '1')
cross join 
(select distinct funnel_type, funnel_stype, install_date, network, country, os from adjust_last where funnel_type = 'loading')c
), 

data_cube_guide as(
select funnel_type, funnel_stype, install_date, network, country, os, step_id, step_num, union_step
from 
(select distinct step_id, step_num, union_step from hive.demo_global_w.dim_gserver_guidestep_stepid where is_compulsory = '1')
cross join 
(select distinct funnel_type, funnel_stype, install_date, network, country, os from adjust_last where funnel_type = 'guide')c
), 

data_cube as(
select * from data_cube_adjust
union all
select * from data_cube_guide
)

select a.funnel_type, a.funnel_stype, 
a.install_date, a.network, a.country, a.os, 
a.event_name, a.step_num, a.union_step, 
e.agg_users, b.demension_users, 
sum(demension_users) over(partition by a.funnel_type, a.funnel_stype, a.install_date, a.network, a.country, a.os order by a.step_num rows between current row and unbounded following) as users, 
nullif(c.normal_users, 0) as normal_users, nullif(c.normal_timediff, 0) as normal_timediff, 
nullif(c.abnormal_users, 0) as abnormal_users, nullif(c.abnormal_timediff, 0) as abnormal_timediff, 
c.timediff_50, c.timediff_75, d.timediffac_50, d.timediffac_75, 
cast(a.install_date as varchar) as part_date
from data_cube a
left join adjust_last b
on a.funnel_type = b.funnel_type 
and a.funnel_stype = b.funnel_stype 
and a.install_date = b.install_date
and a.network = b.network 
and a.country = b.country 
and a.os = b.os 
and a.union_step = b.union_step 
left join adjust_rank_cal c
on a.funnel_type = c.funnel_type 
and a.funnel_stype = c.funnel_stype 
and a.install_date = c.install_date
and a.network = c.network 
and a.country = c.country 
and a.os = c.os 
and a.union_step = c.union_step 
left join adjust_rank_percal d
on a.funnel_type = d.funnel_type 
and a.funnel_stype = d.funnel_stype 
and a.install_date = d.install_date
and a.network = d.network 
and a.country = d.country 
and a.os = d.os 
and a.union_step = d.union_step 
left join adjust_demension_agg e
on a.funnel_type = e.funnel_type 
and a.funnel_stype = e.funnel_stype 
and a.install_date = e.install_date
and a.network = e.network 
and a.country = e.country 
and a.os = e.os 