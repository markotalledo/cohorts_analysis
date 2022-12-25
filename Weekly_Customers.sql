CREATE OR REPLACE VIEW SNP_SANDBOX.COHORTS_WEEKLY_CUSTOMERS AS
(
with leader_tier as
(select case when t.store_url='gruporomost' then 'gruporomo'
 when m.leader_class='mayorista_test_st' then storeurl_origin
 else t.store_url end as store_url,
min(activation_date) activation_date,
max(calendar_month) calendar_month,
max(case when tier like 'mayorista%' then 1 else 0 end) mayorista_f,
max(case when tier='top' then 1 else 0 end) top_f,
max(case when tier='mid' then 1 else 0 end) mid_f,
max(case when tier='basic_potential' then 1 else 0 end) basic_potential_f,
max(case when tier='basic' then 1 else 0 end) basic_f,
case when mayorista_f=1 then 'mayorista'
when top_f=1 then 'top'
when mid_f=1 then 'mid'
when basic_potential_f=1 then 'basic_potential'
else 'basic' end as tier
 from FAVODATA.SNP_SANDBOX.AN_ENTREP_TIER_TEST t
 left join FAVODATA.SNP_UNTRUSTED.MAYORISTAS m on m.dynamo_leader_id=t.dynamo_leader_id and m.leader_class<>'top_mayorista'
group by 1),

customer_tier
as
(select
 dynamo_customer_id,
 case when t.store_url='gruporomost' then 'gruporomo'
 when m.leader_class='mayorista_test_st' then storeurl_origin
 else t.store_url end as store_url_adj,
 lt.tier as leader_tier,
 lt.activation_date as leader_activation_date,
 min(t.activation_date) customer_activation_date,
 max(case when t.tier='leader' then 1 else 0 end) leader_f,
  max(case when t.tier='heavy' then 1 else 0 end) heavy_f,
 max(case when t.tier='mid' then 1 else 0 end) mid_fa,
 max(case when t.tier='low' then 1 else 0 end) low_f,
 case when leader_f=1 then 'leader'
 when heavy_f=1 then 'heavy'
 when mid_fa=1 then 'mid'
 else 'low' end as customer_tier
from favodata.snp_sandbox.an_customer_tier_test t
 left join FAVODATA.SNP_UNTRUSTED.MAYORISTAS m on t.store_url=m.storeurl and m.leader_class<>'top_mayorista'
 left join leader_tier lt on lt.store_url=store_url_adj
 group by 1,2,3,4
),
cohort_pre as
(select distinct
 b.dynamo_customer_id,
 t.store_url_adj,
 leader_tier,
 customer_tier,
 date_trunc('week',leader_activation_date) leader_activation_week,
 case when leader_tier='mayorista' then 'mayorista' else 'no mayorista' end as cluster,
 date_trunc('week',customer_activation_date) as customer_activation_week,
  date_trunc('week',create_date_time_tz::date) calendar_week,
 ceil(datediff('day',customer_activation_date,create_date_time_tz::date)/7) relative_week,
sum(case when coupon like any ('FAV-%','POC-FAV-%','MAY-%','POC-MAY-%') then net_value else gross_value end)
 over (partition by b.dynamo_customer_id,t.store_url_adj,leader_tier,customer_tier,leader_activation_week,cluster,customer_activation_week,calendar_week,relative_week) as
 gmv
,count(distinct order_number)
    over (partition by b.dynamo_customer_id,t.store_url_adj,leader_tier,customer_tier,leader_activation_week,cluster,customer_activation_week,calendar_week,relative_week) as orders,
sum(quantity_fixed)
    over (partition by b.dynamo_customer_id,t.store_url_adj,leader_tier,customer_tier,leader_activation_week,cluster,customer_activation_week,calendar_week,relative_week) as units,
sum(case when create_date_time_tz::date between customer_activation_date and dateadd('day',7,customer_activation_date)
     then (case when coupon like any ('FAV-%','POC-FAV-%','MAY-%','POC-MAY-%') then net_value else gross_value end)
     else 0 end
    ) over (partition by b.dynamo_customer_id) as ticket_1w,
 case when ticket_1w<100 then 'a.Menos de 100 soles' else 'b.Mas de 100 soles' end as tag_ticket_1w

 from (select *,case when item_pack_sku is not null then
 row_number() over (partition by order_number,item_pack_sku order by quantity) else 1 end as n_row,case when n_row=1 then ifnull(item_pack_quantity,quantity) else 0 end as quantity_fixed
 from
 journey.base) b
 left join customer_tier t on t.dynamo_customer_id=b.dynamo_customer_id
where b.country='PE' and b.order_status<>'CANCEL' and b.cancel_date_time_tz is null
 and t.store_url_adj not in (select store from FAVODATA.GROWTH_UNTRUSTED.FRAUDE where country='peru')
 and t.store_url_adj not in (select store from FAVODATA.SNP_SANDBOX.CT_OCT_FRAUD)
 and relative_week > 0
-- group by 1,2,3,4,5,6,7,8,9
)

select
    concat(weekofyear(customer_activation_week), ' - ',year(customer_activation_week)) as weeknum_activation_customer,
    concat(weekofyear(leader_activation_week), ' - ',year(leader_activation_week)) as weeknum_activation_leader,
    * from (
select
customer_activation_week,
leader_activation_week,
relative_week,
cluster,
leader_tier,
customer_tier,
tag_ticket_1w,
--count(distinct case when relative_month=0 then dynamo_customer_id else null end) over (partition by customer_activation_month,leader_activation_month,cluster,leader_tier,customer_tier,tag_ticket_1m) as customers_total,
count(distinct dynamo_customer_id) as customers,
--sum(case when relative_month=0 then gmv else 0 end) over (partition by customer_activation_month,leader_activation_month,cluster,leader_tier,customer_tier,tag_ticket_1m) as gmv_total,
round(sum(gmv),2) as gmv,
sum(orders) as orders,
sum(units) as units,
avg(ticket_1w) as avg_ticket_1w
-- sum(customers) as customers
from cohort_pre
where calendar_week<date_trunc('week',current_date)
group by 1,2,3,4,5,6,7)

    );
