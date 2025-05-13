
--分摊逻辑
--1.广告花费：按network_name、campaign_id、adgroup_id、os_name、install_date、country 这6项进行分组，计算每个广告系列的总广告成本；
--2.直接分摊：将每个系列广告花费与其引入的用户直接匹配，按人均摊；
--3.层级分摊：存在系列广告花费但无匹配的引入用户的情况，需将这部分的广告花费均摊到上一层级引入的用户的身上；
--4.用户广告花费=直接分摊+层级分摊。

###
drop table if exists hive.demo_global_w.dws_adjust_cost_detail_df;
create table if not exists hive.demo_global_w.dws_adjust_cost_detail_df(
role_id varchar,
distinct_id varchar,
time timestamp(3),
event_name varchar,
install_date date,
network_name varchar,
campaign_id varchar,
adgroup_id varchar,
country varchar,
os_name varchar,
campaign_name varchar,
adgroup_name varchar,
creative_name varchar,
event_id varchar,
ad_cost decimal(36, 2),
ad_cost_noc_apportion decimal(36, 2),
ad_cost_no_apportion decimal(36, 2),
ad_cost_n_apportion decimal(36, 2)
);

insert into hive.demo_global_w.dws_adjust_cost_detail_df
(role_id, distinct_id , time, event_name, install_date, 
network_name, campaign_id, adgroup_id, country, os_name, campaign_name, adgroup_name, creative_name, 
event_id, ad_cost, ad_cost_noc_apportion, ad_cost_no_apportion, ad_cost_n_apportion)



-- 广告花费表，按 network_name、campaign_id、adgroup_id、os_name、install_date、country 这6项进行分组，计算每个广告系列的总广告成本，
with adjustcost as(
select campaign_id, adgroup_id, os_name, "day" as install_date, country_code as country, 
(case when network like '%Facebook%' then 'Facebook'
            when network like '%Google Ads%' then 'Google Ads ACI'
            else network end) as network_name,
sum(cast(cost as decimal(10, 4))) as cost
from kudu.demo_global_r.dwd_adjust_cost_live
where day >= '2025-01-01'
group by 1, 2, 3, 4, 5, 6
),

-- user_info表，获得每个玩家的6项数据，用于后续的广告成本分摊       
userinfo as( 
select role_id, 
install_ts, cast(install_date as varchar)as install_date,
network as network_name, os as os_name, country, campaign_id, adgroup_id, 
campaign as campaign_name, adgroup as adgroup_name, creative as creative_name
from hive.demo_global_w.dws_user_info_di
where role_id is not null),

-- 根据user_info中的信息  按照各字段将用户分为以下4类, 所有都有(6项都有)，有noc的用户(install_date、network_name、os_name、country)，有no的用户, 有n的用户  
userinfo_d_user as (
select install_date, network_name, os_name, country, campaign_id, adgroup_id, count(*) as user_count
from userinfo
group by 1, 2, 3, 4, 5, 6 
),

userinfo_noc_user as(
select install_date, network_name, os_name, country, count(*) as noc_user_count
from userinfo
group by 1, 2, 3, 4
),

userinfo_no_user as (
select install_date, network_name, os_name, count(*) as no_user_count
from userinfo
group by 1, 2, 3
),

userinfo_n_user as(
select install_date, network_name, count(*) as n_user_count
from userinfo
group by 1, 2
),

--优先匹配满足所有条件用户，直接分摊匹配的广告费用
adjustcostavg_1 as 
(select a.network_name, a.campaign_id, a.adgroup_id, a.install_date, a.country, a.os_name, a.cost, 
user_count, (case when user_count > 0 then a.cost / user_count else 0 end) as avg_cost
from adjustcost a
left join userinfo_d_user d_user
on a.network_name = trim(d_user.network_name) and a.campaign_id = d_user.campaign_id and a.adgroup_id = d_user.adgroup_id
and a.os_name = d_user.os_name and a.install_date = d_user.install_date and a.country = d_user.country
),

--次匹配满足noc条件用户，上一层级分摊在adjustcostavg_1中未分摊的广告费用
adjustcostavg_2 as 
(select a.*, noc_user_count, (case when coalesce(a.user_count, 0) = 0 and noc_user_count > 0 then a.cost / noc_user_count
      else 0 end) as noc_avg_cost
from adjustcostavg_1 a
left join userinfo_noc_user noc_user
on a.network_name = noc_user.network_name and a.install_date = noc_user.install_date and a.country = noc_user.country
and a.os_name = noc_user.os_name
),

--次匹配满足no条件用户，分摊在adjustcostavg_2中未分摊的广告费用
adjustcostavg_3 as 
(select a.*, no_user_count, (case when coalesce(a.user_count, 0) = 0 and coalesce(a.noc_user_count, 0) = 0 and no_user_count > 0
           then a.cost / no_user_count
      else 0 end) as no_avg_cost
from adjustcostavg_2 a
left join userinfo_no_user no_user
on a.network_name = no_user.network_name and a.install_date = no_user.install_date and a.os_name = no_user.os_name
),

--次匹配满足n条件用户，分摊在adjustcostavg_3中未分摊的广告费用
adjustcostavg_4 as 
(select a.*, n_user_count, (case when coalesce(a.user_count, 0) = 0 and coalesce(a.noc_user_count, 0) = 0 and coalesce(a.no_user_count, 0) = 0
           and n_user_count > 0 then a.cost / n_user_count
      else 0 end) as n_avg_cost
from adjustcostavg_3 a 
left join userinfo_n_user n_user
on a.network_name = n_user.network_name and a.install_date = n_user.install_date
),

--以上条件都未匹配上用户，记为未匹配广告花费
adjustcostavg as 
(select *, (case when coalesce(user_count, 0) = 0 and coalesce(noc_user_count, 0) = 0 and coalesce(no_user_count, 0) = 0
           and coalesce(n_user_count, 0) = 0 then cost
      else 0 end) as not_match_cost
from adjustcostavg_4
),

-- 计算各层级的分摊的最终广告成本，这些分摊结果会在后续的查询中用于对各类型用户匹配。(理论上各类型只有一条数据，不需要sum)
aca_noc as (
select install_date, network_name, os_name, country,
sum(noc_avg_cost) ad_cost_noc_apportion
from adjustcostavg aca
where noc_avg_cost > 0
group by 1, 2, 3, 4
),

aca_no as (
select install_date, network_name, os_name, 
sum(no_avg_cost) ad_cost_no_apportion
from adjustcostavg acan
where no_avg_cost > 0
group by 1, 2, 3
),

aca_n as (
select install_date, network_name, 
sum(n_user_count) ad_cost_n_apportion
from adjustcostavg aca
where n_avg_cost > 0
group by 1, 2
),

-- 对各类型用户进行广告分摊成本的匹配。
matchresulttable as (
select u.role_id, 
date_format(u.install_ts, '%Y-%m-%d %h:%i:%s') as "#time", 'adjust_cost_detail' as "#event_name", 
u.install_date, u.network_name, u.campaign_id, u.adgroup_id, u.country, u.os_name, u.campaign_name, u.adgroup_name, u.creative_name,
concat(u.role_id, '', u.install_date, u.network_name, coalesce(u.campaign_id, ''), coalesce(u.adgroup_id, ''), coalesce(u.country, ''), coalesce(u.os_name, '')) as "#event_id",
coalesce(aca.avg_cost, 0) as ad_cost,
coalesce(aca_noc.ad_cost_noc_apportion, 0) as ad_cost_noc_apportion,
coalesce(aca_no.ad_cost_no_apportion, 0) as ad_cost_no_apportion,
coalesce(aca_n.ad_cost_n_apportion, 0) as ad_cost_n_apportion
from userinfo u
left join adjustcostavg aca
on u.network_name = aca.network_name and u.campaign_id = aca.campaign_id and u.adgroup_id = aca.adgroup_id and u.install_date = aca.install_date 
and u.os_name = aca.os_name and u.country = aca.country
left join aca_noc
on u.network_name = aca_noc.network_name and u.install_date = aca_noc.install_date and u.os_name = aca_noc.os_name and u.country = aca_noc.country
left join aca_no
on u.network_name = aca_no.network_name and u.install_date = aca_no.install_date and u.os_name = aca_no.os_name
left join aca_n
on u.network_name = aca_n.network_name and u.install_date = aca_n.install_date
),

-- 生成未匹配的广告分摊成本用户数据。
notmatchresulttable as(
select null as role_id,
concat(install_date, ' 00:00:00') as "#time", 'adjust_cost_detail' as "#event_name",
install_date, network_name, campaign_id, adgroup_id, country, os_name,
'' as campaign_name, '' as adgroup_name, '' as creative_name,
concat('', 'adjust_not_match_cost', install_date, network_name, coalesce(campaign_id, ''), coalesce(adgroup_id, ''), coalesce(country, ''), coalesce(os_name, '')) as "#event_id",
not_match_cost as ad_cost, 0 as ad_cost_noc_apportion, 0 as ad_cost_no_apportion, 0 as ad_cost_n_apportion
from adjustcostavg a
where not_match_cost > 0
),

-- 将匹配的广告成本数据和不匹配的数据合并，形成最终结果表。  
resulttable as(
select * from matchresulttable
union all
select * from notmatchresulttable
)

select role_id, null as distinct_id , cast("#time" as timestamp) as time, 
"#event_name" as event_name, cast(install_date as date) as install_date, 
network_name, campaign_id, adgroup_id, country, os_name, campaign_name, adgroup_name, creative_name, 
"#event_id" as event_id, ad_cost, ad_cost_noc_apportion, ad_cost_no_apportion, ad_cost_n_apportion
from resulttable
###