create table if not exists hive.demo_global_w.dws_user_daily_derive_di(
date date, 
role_id varchar, 
login_days bigint, 
is_new bigint,
is_firstpay bigint, 
is_pay bigint, 
is_paid bigint,
money_ac decimal(36, 2), 
appmoney_ac decimal(36, 2), 
webmoney_ac decimal(36, 2), 
sincetimes_end bigint, 
core_end bigint, 
free_end bigint, 
paid_end bigint, 
before_date date, 
after_date date, 
part_date varchar
)
with(
partitioned_by = array['part_date']
);

delete from hive.demo_global_w.dws_user_daily_derive_di 
where part_date >= date_format(date_add('day', -15, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}';

insert into hive.demo_global_w.dws_user_daily_derive_di(
date, role_id, login_days, 
is_new, is_firstpay, is_pay, is_paid, 
money_ac, appmoney_ac, webmoney_ac, 
sincetimes_end, core_end, free_end, paid_end, 
before_date, after_date, part_date
)

with active_role as(
select distinct role_id
from hive.demo_global_w.dws_user_daily_di
where part_date >= date_format(date_add('day', -15, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}'
), 

user_daily as(
select date, role_id, 
row_number() over(partition by role_id order by part_date) as login_days, 
firstpay_ts, money, app_money, web_money, 
sincetimes_end, core_end, free_end, paid_end, 
part_date
from hive.demo_global_w.dws_user_daily_di a
where exists
(select 1 from active_role b
where a.role_id = b.role_id)
), 

daily_cal as(
select date, role_id, login_days, 
money, 
min(date) over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as install_date, 
min(firstpay_ts) over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as firstpay_ts, 
sum(money) over(partition by role_id order by part_date rows between unbounded preceding and current row) as money_ac, 
sum(app_money) over(partition by role_id order by part_date rows between unbounded preceding and current row) as appmoney_ac, 
sum(web_money) over(partition by role_id order by part_date rows between unbounded preceding and current row) as webmoney_ac, 
last_value(sincetimes_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and current row) as sincetimes_end,
last_value(core_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and current row) as core_end, 
last_value(free_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and current row) as free_end, 
last_value(paid_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and current row) as paid_end, 
part_date
from user_daily
), 

daily_boolean_cal as(
select date, role_id, login_days, 
(case when date = install_date then 1 else 0 end) as is_new, 
(case when date(firstpay_ts) = date(part_date) then 1 else 0 end) as is_firstpay, 
(case when money > 0 then 1 else 0 end) as is_pay, 
(case when money_ac > 0 then 1 else 0 end) as is_paid, 
money_ac, appmoney_ac, webmoney_ac, 
sincetimes_end, core_end, free_end, paid_end, 
lag(date, 1, install_date) over(partition by role_id order by date) as before_date,
lead(date, 1, null) over(partition by role_id order by date) as after_date, 
part_date
from daily_cal
)

select
date, role_id, login_days, 
is_new, is_firstpay, is_pay, is_paid, 
money_ac, appmoney_ac, webmoney_ac, 
sincetimes_end, core_end, free_end, paid_end, 
before_date, after_date, part_date
from daily_boolean_cal
where part_date >= date_format(date_add('day', -15, date '{yesterday}'), '%Y-%m-%d')
and part_date <= '{today}'