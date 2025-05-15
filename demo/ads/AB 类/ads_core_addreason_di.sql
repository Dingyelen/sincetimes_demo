create table if not exists hive.demo_global_w.ads_core_addreason_di
(date date,
zone_id varchar,
channel varchar,
reason varchar,
core_add bigint,
users bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.demo_global_w.ads_core_addreason_di
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}';

insert into hive.demo_global_w.ads_core_addreason_di
(date, zone_id, channel,
reason, core_add, users,
part_date)

with dws_core_daily as(
select date, role_id, coreadd_detail, corecost_detail, core_end, part_date
from hive.demo_global_w.dws_core_snapshot_di
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}'
),

dws_core_add_reason as(
select date, part_date, role_id, reason, core_add
from dws_core_daily
cross join unnest(cast(json_parse(coreadd_detail) as map(varchar, bigint))) as addinfo(reason, core_add)
),

dws_core_daily_join as(
select a.date, a.part_date, a.role_id, a.reason, a.core_add,
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel,
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, b.firstpay_date) as firstpay_interval_days
from dws_core_add_reason a
left join hive.demo_global_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test = 0
),

core_daily_agg as(
select date, part_date, zone_id, channel, 
reason,
sum(core_add) as core_add,
count(distinct role_id) as users
from dws_core_daily_join
group by 1, 2, 3, 4, 5
)

select date, zone_id, channel,
reason, core_add, users,
part_date
from core_daily_agg
;