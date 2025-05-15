drop table if exists hive.demo_global_w.ads_kpi_server_df;

create table if not exists hive.demo_global_w.ads_kpi_server_df(
zone_id varchar, 
open_date date, 
yesterday_new_users bigint,
yesterday_active_users bigint,
yesterday_moneyrmb decimal(36, 2), 
new_users bigint,
active_users bigint,
money decimal(36, 2),
money_rmb decimal(36, 2),
newusers_ac bigint,
money_ac decimal(36, 2),
moneyrmb_ac decimal(36, 2),
leveltop_roleid varchar,
leveltop_level bigint,
leveltop_moneyrmb decimal(36, 2),
paytop_roleid varchar,
paytop_level bigint,
paytop_moneyrmb decimal(36, 2)
);

insert into hive.demo_global_w.ads_kpi_server_df(
zone_id, open_date, 
yesterday_new_users, yesterday_active_users, yesterday_moneyrmb, 
new_users, active_users, money, money_rmb,
newusers_ac, money_ac, moneyrmb_ac, 
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb
)

with server_daily as(
select date, part_date, zone_id, open_date, 
new_users, active_users, firstpay_users, money, money_rmb, pay_count, newusers_ac, money_ac, moneyrmb_ac
from hive.demo_global_w.dws_server_daily_df
where part_date >= date_format(date_add('day', -15, current_date), '%Y-%m-%d')
and part_date <= date_format(current_date, '%Y-%m-%d')
), 

server_all_agg as(
select zone_id, open_date, 
sum(new_users) as new_users, 
sum(active_users) as active_users, 
sum(money) as money, 
sum(money_rmb) as money_rmb, 
max(newusers_ac) as newusers_ac, 
max(money_ac) as money_ac,  
sum(moneyrmb_ac) as moneyrmb_ac
from server_daily
group by 1, 2
), 

server_yesterday_agg as(
select zone_id, 
sum(new_users) as yesterday_new_users, 
sum(active_users) as yesterday_active_users, 
sum(money_rmb) as yesterday_moneyrmb
from server_daily
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
group by 1
), 

user_info as(
select 
zone_id, role_id, 
level, moneyrmb_ac, 
row_number() over(partition by zone_id order by level desc, money_ac desc) as level_desc, 
row_number() over(partition by zone_id order by money_ac desc, level desc) as money_desc
from hive.demo_global_w.dws_user_info_di
), 

user_level_select as(
select zone_id, role_id as leveltop_roleid, 
level as leveltop_level, moneyrmb_ac as leveltop_moneyrmb
from user_info
where level_desc = 1
), 

user_money_select as(
select zone_id, role_id as paytop_roleid, 
level as paytop_level, moneyrmb_ac as paytop_moneyrmb
from user_info
where money_desc = 1
)

select a.zone_id, a.open_date, 
b.yesterday_new_users, b.yesterday_active_users, b.yesterday_moneyrmb, 
a.new_users, a.active_users, a.money, a.money_rmb,
a.newusers_ac, a.money_ac, a.moneyrmb_ac, 
c.leveltop_roleid, c.leveltop_level, c.leveltop_moneyrmb, 
d.paytop_roleid, d.paytop_level, d.paytop_moneyrmb
from server_all_agg a
left join server_yesterday_agg b
on a.zone_id = b.zone_id
left join user_level_select c
on a.zone_id = c.zone_id
left join user_money_select d
on a.zone_id = d.zone_id
