drop table if exists hive.demo_global_w.dws_server_daily_df;

create table if not exists hive.demo_global_w.dws_server_daily_df
(date date,
zone_id varchar,
open_date date, 
online_time bigint,
new_users bigint,
active_users bigint,
paid_users bigint,
pay_users bigint,
web_users bigint,
firstpay_users bigint,
firstdaypay_users bigint,
money decimal(36, 2),
app_money decimal(36, 2),
web_money decimal(36, 2),
money_rmb decimal(36, 2),
app_moneyrmb decimal(36, 2),
web_moneyrmb decimal(36, 2),
pay_count bigint,
app_count bigint,
web_count bigint,
first_pay decimal(36, 2),
firstday_pay decimal(36, 2),
newusers_ac bigint, 
payusers_ac bigint, 
money_ac decimal(36, 2), 
moneyrmb_ac decimal(36, 2), 
leveltop_roleid varchar,
leveltop_level bigint,
leveltop_moneyrmb decimal(36, 2),
paytop_roleid varchar,
paytop_level bigint,
paytop_moneyrmb decimal(36, 2),
retention_day bigint,
part_date varchar
)
with(format = 'ORC',
transactional = true,
partitioned_by = array['part_date']
);

insert into hive.demo_global_w.dws_server_daily_df
(date, zone_id, open_date, 
online_time, 
new_users, active_users, 
paid_users, pay_users, web_users, 
firstpay_users, firstdaypay_users, 
money, app_money, web_money, 
money_rmb, app_moneyrmb, web_moneyrmb, 
pay_count, app_count, web_count, 
first_pay, firstday_pay, 
newusers_ac, payusers_ac, money_ac, moneyrmb_ac, 
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb, 
retention_day, part_date)

with user_daily as(
select date, part_date, role_id, 
level_min, level_max, viplevel_min, viplevel_max, 
online_time, money, app_money, web_money, 
money * rate as money_rmb, app_money * rate as app_moneyrmb, web_money * rate as web_moneyrmb, 
pay_count, app_count, web_count
from hive.demo_global_w.dws_user_daily_di a
left join mysql_bi_r."gbsp-bi-bigdata".t_currency_rate b
on a.currency = b.currency and date_format(a.date, '%Y-%m') = b.currency_time 
), 

user_daily_derive as(
select date, part_date, role_id, 
is_new, is_firstpay, is_pay, is_paid
from hive.demo_global_w.dws_user_daily_derive_di
),

user_daily_join as
(select a.date, a.part_date, a.role_id, z.zone_id, 
a.level_min, a.level_max, a.viplevel_min, a.viplevel_max, 
b.is_new, b.is_firstpay, b.is_pay, b.is_paid, 
a.online_time, 
a.money, a.app_money, a.web_money, 
a.money_rmb, a.app_moneyrmb, a.web_moneyrmb, 
a.pay_count, a.app_count, a.web_count
from user_daily a
left join user_daily_derive b
on a.role_id = b.role_id and a.part_date = b.part_date
left join hive.demo_global_w.dws_user_info_di z
on a.role_id = z.role_id
where z.is_test = 0
),

daily_info as
(select date, part_date, zone_id, 
sum(online_time) as online_time,    
sum(is_new) as new_users, 
count(distinct role_id) as active_users, 
sum(is_paid) as paid_users, 
sum(is_pay) as pay_users, 
count(distinct case when web_money > 0 then role_id else null end) as web_users, 
sum(is_firstpay) as firstpay_users, 
count(distinct case when is_firstpay = 1 and is_new = 1 then role_id else null end) as firstdaypay_users, 
sum(money) as money, 
sum(app_money) as app_money, 
sum(web_money) as web_money,
sum(money_rmb) as money_rmb, 
sum(app_moneyrmb) as app_moneyrmb, 
sum(web_moneyrmb) as web_moneyrmb, 
sum(pay_count) as pay_count, 
sum(app_count) as app_count, 
sum(web_count) as web_count, 
sum(case when is_firstpay = 1 then money else null end) as first_pay, 
sum(case when is_firstpay = 1 and is_new = 1 then money else null end) as firstday_pay
from user_daily_join
group by 1, 2, 3
),

user_daily_rn as(
select date, part_date, role_id, 
zone_id, level_max, money, money_rmb, 
row_number() over(partition by date, zone_id order by level_max desc, money desc) as level_desc, 
row_number() over(partition by date, zone_id order by money desc, level_max desc) as money_desc
from user_daily_join
), 

server_info as
(select a.date, a.part_date, a.zone_id, 
a.online_time, 
a.new_users, a.active_users, 
a.paid_users, a.pay_users, a.web_users, a.firstpay_users, a.firstdaypay_users, 
a.money, a.app_money, a.web_money, a.money_rmb, a.app_moneyrmb, a.web_moneyrmb, 
a.pay_count, a.app_count, a.web_count, a.first_pay, a.firstday_pay, 
b.role_id as leveltop_roleid, b.level_max as leveltop_level, b.money_rmb as leveltop_moneyrmb, 
c.role_id as paytop_roleid, c.level_max as paytop_level, c.money_rmb as paytop_moneyrmb
from daily_info a
left join user_daily_rn b
on a.date = b.date and a.zone_id = b.zone_id 
left join user_daily_rn c
on a.date = c.date and a.zone_id = c.zone_id 
where b.level_desc = 1
and c.money_desc = 1
), 

server_win_info as(
select date, part_date, zone_id,
online_time, 
new_users, active_users, 
paid_users, pay_users, web_users, firstpay_users, firstdaypay_users, 
money, app_money, web_money, money_rmb, app_moneyrmb, web_moneyrmb, 
pay_count, app_count, web_count, first_pay, firstday_pay, 
sum(new_users) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as newusers_ac, 
sum(firstpay_users) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as payusers_ac, 
sum(money) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as money_ac, 
sum(money_rmb) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as moneyrmb_ac, 
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb
from server_info
), 

open_date as(
select min(date) as open_date
from server_win_info
where newusers_ac >= 0
), 

server_daily_res as(
select date, zone_id, (select open_date from open_date) as open_date, 
online_time, 
new_users, active_users, 
paid_users, pay_users, web_users, 
firstpay_users, firstdaypay_users, 
money, app_money, web_money, 
money_rmb, app_moneyrmb, web_moneyrmb, 
pay_count, app_count, web_count, 
first_pay, firstday_pay, 
newusers_ac, payusers_ac, money_ac, moneyrmb_ac, 
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb, 
part_date
from server_win_info 
)

select date, zone_id, open_date, 
online_time, 
new_users, active_users, 
paid_users, pay_users, web_users, 
firstpay_users, firstdaypay_users, 
money, app_money, web_money, 
money_rmb, app_moneyrmb, web_moneyrmb, 
pay_count, app_count, web_count, 
first_pay, firstday_pay, 
newusers_ac, payusers_ac, money_ac, moneyrmb_ac, 
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb, 
date_diff('day', open_date, date) as retention_day, 
part_date
from server_daily_res
;