create table if not exists hive.demo_global_w.dws_user_info_di(
role_id varchar,
device_id varchar,
open_id varchar,
adid varchar,
zone_id varchar,
alliance_id varchar,
os varchar, 
channel varchar, 
ip varchar,
country varchar,
network varchar,
campaign varchar,
creative varchar,
adgroup varchar,
campaign_id varchar,
creative_id varchar,
adgroup_id varchar,
adcost decimal(36, 2),
is_test bigint,
install_ts timestamp(3),
install_date date,
lastlogin_ts timestamp(3),
currency varchar, 
firstpay_ts timestamp(3),
firstpay_date date,
firstpay_level bigint,
firstpay_goodid varchar,
firstpay_money decimal(36, 2),
lastpay_ts timestamp(3),
lastpay_level bigint,
lastpay_goodid varchar,
lastpay_money decimal(36, 2),
is_paid bigint,
money_ac decimal(36, 2),
appmoney_ac decimal(36, 2),
webmoney_ac decimal(36, 2),
moneyrmb_ac decimal(36, 2),
appmoneyrmb_ac decimal(36, 2),
webmoneyrmb_ac decimal(36, 2),
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
vip_level bigint,
level bigint,
rank bigint,
power bigint, 
login_days bigint,
login_times bigint,
online_time bigint
)
with(
format = 'ORC',
transactional = true
);

delete from hive.demo_global_w.dws_user_info_di 
where exists(
select 1
from hive.demo_global_w.dws_user_daily_di
where dws_user_daily_di.role_id = dws_user_info_di.role_id
and dws_user_daily_di.part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and dws_user_daily_di.part_date <= '{today}'
);

drop table if exists hive.demo_global_w.temp_user_daily;

create table if not exists hive.demo_global_w.temp_user_daily as
select a.*, 
a.money * b.rate as moneyrmb_ac, app_money * b.rate as appmoneyrmb_ac, web_money * b.rate as webmoneyrmb_ac
from hive.demo_global_w.dws_user_daily_di a
left join mysql_bi_r."gbsp-bi-bigdata".t_currency_rate b
on a.currency = b.currency and date_format(a.date, '%Y-%m') = b.currency_time 
where exists
(select 1
from hive.demo_global_w.dws_user_daily_di z
where part_date >= date_format(date_add('day', -6, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}'
and a.role_id = z.role_id
);

drop table if exists hive.demo_global_w.temp_user_info;

create table if not exists hive.demo_global_w.temp_user_info as
select role_id, 
max(is_test) as is_test,
min(first_ts) as install_ts,
max(last_ts) as lastlogin_ts,
max(viplevel_max) as vip_level, 
max(level_max) as level, 
max(rank_max) as rank, 
max(power_max) as power, 
count(*) as login_days, 
sum(online_time) as online_time, 
sum(login_times) as login_times, 
sum(money) as money_ac, 
sum(app_money) as appmoney_ac, 
sum(web_money) as webmoney_ac, 
sum(moneyrmb_ac) as moneyrmb_ac, 
sum(appmoneyrmb_ac) as appmoneyrmb_ac, 
sum(webmoneyrmb_ac) as webmoneyrmb_ac, 
sum(pay_count) as pay_count, 
sum(app_count) as app_count, 
sum(web_count) as web_count, 
sum(sincetimes_add) as sincetimes_add, 
sum(sincetimes_cost) as sincetimes_cost, 
sum(core_add) as core_add, 
sum(core_cost) as core_cost, 
sum(free_add) as free_add, 
sum(free_cost) as free_cost, 
sum(paid_add) as paid_add, 
sum(paid_cost) as paid_cost, 
min(firstpay_ts) as firstpay_ts, 
min(firstpay_level) as firstpay_level, 
max(lastpay_ts) as lastpay_ts, 
max(lastpay_level) as lastpay_level
from hive.demo_global_w.temp_user_daily 
group by 1;

drop table if exists hive.demo_global_w.temp_user_first_info;

create table if not exists hive.demo_global_w.temp_user_first_info as
select distinct role_id, 
first_value(device_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as device_id,
first_value(open_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as open_id,
first_value(adid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as adid,
first_value(zone_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as zone_id,
last_value(alliance_id) over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as alliance_id,
first_value(os) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as os,
first_value(channel) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as channel,
first_value(ip) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as ip,
first_value(country) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as country,
first_value(network) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as network,
first_value(campaign) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as campaign,
first_value(creative) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as creative,
first_value(adgroup) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as adgroup,
first_value(campaign_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as campaign_id,
first_value(creative_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as creative_id,
first_value(adgroup_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as adgroup_id, 
first_value(currency) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as currency,
first_value(firstpay_goodid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as firstpay_goodid,
first_value(firstpay_money) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as firstpay_money,
last_value(lastpay_goodid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as lastpay_goodid,
last_value(lastpay_money) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as lastpay_money,
last_value(sincetimes_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as sincetimes_end,
last_value(core_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as core_end,
last_value(free_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as free_end,
last_value(paid_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as paid_end
from hive.demo_global_w.temp_user_daily;

drop table if exists hive.demo_global_w.temp_user_adcost;

create table if not exists hive.demo_global_w.temp_user_adcost as
select role_id, 
sum(ad_cost + ad_cost_noc_apportion + ad_cost_no_apportion + ad_cost_n_apportion) as adcost
from hive.demo_global_w.dws_adjust_cost_detail_df
group by 1;

insert into hive.demo_global_w.dws_user_info_di(
role_id, device_id, open_id, adid, 
zone_id, alliance_id, os, channel, 
ip, country, network, campaign, creative, adgroup, 
campaign_id, creative_id, adgroup_id, adcost, 
is_test, install_ts, install_date, lastlogin_ts, 
currency, firstpay_ts, firstpay_date, 
firstpay_level, firstpay_goodid, firstpay_money, 
lastpay_ts, lastpay_level, lastpay_goodid, lastpay_money, is_paid, 
money_ac, appmoney_ac, webmoney_ac, 
moneyrmb_ac, appmoneyrmb_ac, webmoneyrmb_ac, 
pay_count, app_count, web_count, 
sincetimes_add, sincetimes_cost, sincetimes_end, 
core_add, core_cost, core_end, 
free_add, free_cost, free_end, 
paid_add, paid_cost, paid_end, 
vip_level, level, rank, power, 
login_days, login_times, online_time
)

select 
a.role_id, coalesce(b.device_id, '') as device_id, coalesce(b.open_id, '') as open_id, coalesce(b.adid, '') as adid, 
coalesce(b.zone_id, '') as zone_id, coalesce(b.alliance_id, '') as alliance_id, coalesce(b.os, '') as os, coalesce(b.channel, '') as channel, 
coalesce(b.ip, '') as ip, coalesce(b.country, '') as country, 
coalesce(b.network, '') as network, coalesce(b.campaign, '') as campaign, coalesce(b.creative, '') as creative, coalesce(b.adgroup, '') as adgroup, 
coalesce(b.campaign_id, '') as campaign_id, coalesce(b.creative_id, '') as creative_id, coalesce(b.adgroup_id, '') as adgroup_id, 
c.adcost, 
a.is_test, 
a.install_ts, 
date(a.install_ts) as install_date, a.lastlogin_ts, 
b.currency, a.firstpay_ts, date(a.firstpay_ts) as firstpay_date, 
a.firstpay_level, b.firstpay_goodid, b.firstpay_money, 
a.lastpay_ts, a.lastpay_level, b.lastpay_goodid, b.lastpay_money, 
(case when money_ac > 0 then 1 else 0 end) as is_paid, 
a.money_ac, a.appmoney_ac, a.webmoney_ac, 
a.moneyrmb_ac, a.appmoneyrmb_ac, a.webmoneyrmb_ac, 
a.pay_count, a.app_count, a.web_count, 
a.sincetimes_add, a.sincetimes_cost, b.sincetimes_end, 
a.core_add, a.core_cost, b.core_end, 
a.free_add, a.free_cost, b.free_end, 
a.paid_add, a.paid_cost, b.paid_end, 
a.vip_level, a.level, a.rank, a.power, 
a.login_days, a.login_times, a.online_time
from hive.demo_global_w.temp_user_info a
left join hive.demo_global_w.temp_user_first_info b
on a.role_id = b.role_id
left join hive.demo_global_w.temp_user_adcost c
on a.role_id = c.role_id;


drop table if exists hive.demo_global_w.temp_user_daily;
drop table if exists hive.demo_global_w.temp_user_info;
drop table if exists hive.demo_global_w.temp_user_first_info;
drop table if exists hive.demo_global_w.temp_user_adcost;