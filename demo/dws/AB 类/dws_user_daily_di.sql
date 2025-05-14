create table if not exists hive.demo_global_w.dws_user_daily_di(
date date, 
role_id varchar, 
device_id varchar, 
open_id varchar, 
adid varchar, 
app_id varchar, 
channel varchar, 
zone_id varchar, 
alliance_id varchar, 
os varchar, 
ip varchar, 
country varchar, 
network varchar, 
campaign varchar, 
creative varchar, 
adgroup varchar, 
campaign_id varchar, 
creative_id varchar, 
adgroup_id varchar, 
first_ts timestamp(3), 
last_ts timestamp(3), 
viplevel_min bigint, 
viplevel_max bigint, 
level_min bigint, 
level_max bigint, 
rank_min bigint, 
rank_max bigint, 
power_min bigint, 
power_max bigint, 
online_time bigint, 
login_times bigint, 
currency varchar, 
firstpay_ts timestamp(3), 
firstpay_level bigint, 
firstpay_goodid varchar, 
firstpay_money decimal(36, 2), 
lastpay_ts timestamp(3), 
lastpay_level bigint, 
lastpay_goodid varchar, 
lastpay_money decimal(36, 2), 
pay_detail array(varchar), 
money decimal(36, 2), 
app_money decimal(36, 2), 
web_money decimal(36, 2), 
pay_count bigint, 
app_count bigint, 
web_count bigint, 
sincetimes_add bigint, 
sincetimes_cost bigint, 
sincetimes_end bigint, 
core_add bigint, 
core_cost bigint, 
core_end bigint, 
free_add bigint, 
free_cost bigint, 
free_end bigint, 
paid_add bigint, 
paid_cost bigint, 
paid_end bigint, 
is_test bigint, 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.dws_user_daily_di 
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}';

insert into  hive.demo_global_w.dws_user_daily_di
(date, role_id, device_id, open_id, adid, 
app_id, channel, zone_id, alliance_id, 
os, ip, country, 
network, campaign, creative, adgroup, 
campaign_id, creative_id, adgroup_id, 
first_ts, last_ts, 
viplevel_min, viplevel_max,
level_min, level_max,
rank_min, rank_max,
power_min, power_max,  
online_time, login_times, 
currency, firstpay_ts, firstpay_level, firstpay_goodid, firstpay_money, 
lastpay_ts, lastpay_level, lastpay_goodid, lastpay_money, 
pay_detail, money, app_money, web_money, 
pay_count, app_count, web_count, 
sincetimes_add, sincetimes_cost, sincetimes_end, 
core_add, core_cost, core_end, 
free_add, free_cost, free_end, 
paid_add, paid_cost, paid_end, 
is_test, part_date)

with base_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, device_id, 
channel, zone_id, alliance_id, app_id, 
vip_level, level, rank_level, power, 
pay_source, payment_itemid, currency, money, 
online_time, 
row_number() over(partition by role_id, part_date, event_name order by event_time) as partevent_rn, 
row_number() over(partition by role_id, part_date, event_name order by event_time desc) as partevent_descrn
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

item_log_base as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
reason, event_type, 
item_id, item_num, item_end
from hive.demo_global_r.dwd_gserver_itemchange_live
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}' 
and item_id = '2'
), 

item_log as(
select part_date, event_name, event_time, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, reason, 
(case when event_type = 'gain' then item_num else null end) as sincetimes_add, 
(case when event_type = 'cost' then item_num else null end) as sincetimes_cost, 
item_end as sincetimes_end
from item_log_base
), 

adjust_log as(
select part_date, activity_kind, event_time, 
role_id, adid, 
os_name as os, 
ip_address as ip, country, 
network_name as network, 
campaign_name as campaign, 
creative_name as creative, 
adgroup_name as adgroup, 
campaign_id, creative_id, adgroup_id 
from hive.demo_global_r.dwd_adjust_live
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}'
), 

daily_gserver_info as(
select part_date, date, role_id, 
min(event_time) as first_ts,
max(event_time) as last_ts,
min(vip_level) as viplevel_min,
max(vip_level) as viplevel_max,
min(level) as level_min,
max(level) as level_max,
min(rank_level) as rank_min,
max(rank_level) as rank_max, 
min(power) as power_min,
max(power) as power_max, 
sum(case when event_name = 'Payment' then 1 else null end) as pay_count, 
sum(case when event_name = 'Payment' and pay_source = 'app' then 1 else null end) as app_count, 
sum(case when event_name = 'Payment' and pay_source = 'web' then 1 else null end) as web_count, 
sum(case when event_name = 'rolelogin' then 1 else null end) as login_times, 
sum(online_time) as online_time
from base_log
group by 1, 2, 3
), 

daily_payment_cal as(
select part_date, date, role_id, currency, 
sum(money) as money, 
sum(case when pay_source = 'app' then money else null end) as app_money, 
sum(case when pay_source = 'web' then money else null end) as web_money
from base_log
where event_name = 'Payment'
group by 1, 2, 3, 4
), 

daily_payment_detail as(
select part_date, date, role_id, 
array_sort(array_agg(json_object('currency': currency, 'money': money, 'app_money': app_money, 'web_money': web_money)), 
(x, y) -> if(json_value(x, 'lax $.currency') < json_value(y, 'lax $.currency'), 1, if(json_value(x, 'lax $.currency') = json_value(y, 'lax $.currency'), 0, -1))) as pay_detail
from daily_payment_cal
group by 1, 2, 3
), 

daily_payment_info as(
select part_date, date, role_id, pay_detail, 
json_value(element_at(pay_detail, 1), 'lax $.currency') as currency, 
cast(json_value(element_at(pay_detail, 1), 'lax $.money') as double) as money, 
cast(json_value(element_at(pay_detail, 1), 'lax $.app_money') as double) as app_money, 
cast(json_value(element_at(pay_detail, 1), 'lax $.web_money') as double) as web_money
from daily_payment_detail
), 

daily_gserver_first_info as(
select distinct part_date, role_id, 
first_value(adid) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as adid, 
first_value(app_id) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as app_id, 
first_value(device_id) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as device_id, 
first_value(open_id) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as open_id, 
first_value(channel) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as channel, 
first_value(zone_id) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as zone_id, 
last_value(alliance_id) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as alliance_id
from base_log
), 

core_gserver_last_info as(
select distinct part_date, role_id, 
last_value(free_end) ignore nulls over(partition by role_id, part_date order by event_time, free_end rows between unbounded preceding and unbounded following) as free_end, 
last_value(paid_end) ignore nulls over(partition by role_id, part_date order by event_time, paid_end rows between unbounded preceding and unbounded following) as paid_end, 
last_value(core_end) ignore nulls over(partition by role_id, part_date order by event_time, core_end rows between unbounded preceding and unbounded following) as core_end 
from core_log
), 

core_gserver_info as(
select part_date, role_id, 
sum(free_add) as free_add, 
sum(paid_add) as paid_add, 
sum(core_add) as core_add, 
sum(free_cost) as free_cost, 
sum(paid_cost) as paid_cost, 
sum(core_cost) as core_cost
from core_log
group by 1, 2
), 

item_gserver_last_info as(
select distinct part_date, role_id, 
last_value(sincetimes_end) ignore nulls over(partition by role_id, part_date order by event_time, sincetimes_end rows between unbounded preceding and unbounded following) as sincetimes_end
from item_log
), 

item_gserver_info as(
select part_date, role_id, 
sum(sincetimes_add) as sincetimes_add, 
sum(sincetimes_cost) as sincetimes_cost
from item_log
group by 1, 2
), 

adjust_first_info_roleid as(
select distinct part_date, role_id, 
first_value(os) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as os, 
first_value(ip) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as ip, 
first_value(country) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as country, 
first_value(network) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as network, 
first_value(campaign) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as campaign, 
first_value(creative) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as creative, 
first_value(adgroup) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as adgroup, 
first_value(campaign_id) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as campaign_id, 
first_value(creative_id) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as creative_id, 
first_value(adgroup_id) ignore nulls over(partition by role_id, part_date order by event_time rows between unbounded preceding and unbounded following) as adgroup_id
from adjust_log
), 

adjust_first_info_adid as(
select distinct part_date, adid, 
first_value(os) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as os, 
first_value(ip) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as ip, 
first_value(country) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as country, 
first_value(network) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as network, 
first_value(campaign) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as campaign, 
first_value(creative) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as creative, 
first_value(adgroup) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as adgroup, 
first_value(campaign_id) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as campaign_id, 
first_value(creative_id) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as creative_id, 
first_value(adgroup_id) ignore nulls over(partition by adid, part_date order by event_time rows between unbounded preceding and unbounded following) as adgroup_id
from adjust_log
), 

first_info as(
select part_date, role_id, 
event_time as firstpay_ts, 
level as firstpay_level, 
payment_itemid as firstpay_goodid, 
currency as firstpay_currency, 
money as firstpay_money 
from base_log
where event_name = 'Payment'
and partevent_rn = 1
), 

last_info as(
select part_date, role_id, 
event_time as lastpay_ts, 
level as lastpay_level, 
payment_itemid as lastpay_goodid, 
currency as lastpay_currency, 
money as lastpay_money 
from base_log
where event_name = 'Payment'
and partevent_descrn = 1
), 

test_info as(
select distinct role_id, 
1 as is_test
from hive.demo_global_w.dim_gserver_base_roleid
), 

daily_info as(
select 
a.date, 
a.role_id, c.device_id, c.open_id, c.adid, 
c.app_id, c.channel, c.zone_id, c.alliance_id, 
coalesce(f.os, g.os) as os, coalesce(f.ip, g.ip) as ip, coalesce(f.country, g.country) as country, 
coalesce(f.network, g.network) as network, 
coalesce(f.campaign, g.campaign) as campaign, coalesce(f.creative, g.creative) as creative, coalesce(f.adgroup, g.adgroup) as adgroup, 
coalesce(f.campaign_id, g.campaign_id) as campaign_id, 
coalesce(f.creative_id, g.creative_id) as creative_id, coalesce(f.adgroup_id, g.adgroup_id) as adgroup_id, 
a.first_ts, a.last_ts, 
a.viplevel_min, a.viplevel_max,
a.level_min, a.level_max,
a.rank_min, a.rank_max,
a.power_min, a.power_max,  
a.online_time, a.login_times, 
b.currency, j.firstpay_ts, j.firstpay_level, j.firstpay_goodid, j.firstpay_money, 
k.lastpay_ts, k.lastpay_level, k.lastpay_goodid, k.lastpay_money, 
b.pay_detail, b.money, b.app_money, b.web_money, 
a.pay_count, a.app_count, a.web_count, 
i.sincetimes_add, i.sincetimes_cost, h.sincetimes_end, 
d.core_add, d.core_cost, e.core_end, 
d.free_add, d.free_cost, e.free_end, 
d.paid_add, d.paid_cost, e.paid_end, 
case when z.is_test is null then 0 end as is_test, a.part_date
from daily_gserver_info a
left join daily_payment_info b
on a.role_id = b.role_id and a.part_date = b.part_date
left join daily_gserver_first_info c
on a.role_id = c.role_id and a.part_date = c.part_date
left join core_gserver_info d
on a.role_id = d.role_id and a.part_date = d.part_date
left join core_gserver_last_info e
on a.role_id = e.role_id and a.part_date = e.part_date
left join adjust_first_info_roleid f
on a.role_id = f.role_id and a.part_date = f.part_date
left join adjust_first_info_adid g
on c.adid = g.adid and a.part_date = g.part_date
left join item_gserver_last_info h
on a.role_id = h.role_id and a.part_date = h.part_date
left join item_gserver_info i
on a.role_id = i.role_id and a.part_date = i.part_date
left join first_info j
on a.role_id = j.role_id and a.part_date = j.part_date
left join last_info k
on a.role_id = k.role_id and a.part_date = k.part_date
left join test_info z
on a.role_id = z.role_id
)

select 
date, role_id, device_id, open_id, adid, 
app_id, channel, zone_id, alliance_id, 
os, ip, country, 
network, campaign, creative, adgroup, 
campaign_id, creative_id, adgroup_id, 
first_ts, last_ts, 
viplevel_min, viplevel_max,
level_min, level_max,
rank_min, rank_max,
power_min, power_max,  
online_time, login_times, 
currency, firstpay_ts, firstpay_level, firstpay_goodid, firstpay_money, 
lastpay_ts, lastpay_level, lastpay_goodid, lastpay_money, 
pay_detail, money, app_money, web_money, 
pay_count, app_count, web_count, 
sincetimes_add, sincetimes_cost, sincetimes_end, 
core_add, core_cost, core_end, 
free_add, free_cost, free_end, 
paid_add, paid_cost, paid_end, 
is_test, part_date
from daily_info;