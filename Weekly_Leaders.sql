CREATE OR REPLACE VIEW SNP_SANDBOX.COHORTS_WEEKLY_LEADERS AS
(
with leader_tier as
         (select case
                     when t.store_url = 'gruporomost' then 'gruporomo'
                     when m.leader_class = 'mayorista_test_st' then storeurl_origin
                     else t.store_url end as                               store_url,
                 min(activation_date)                                      activation_date,
                 max(case when tier like 'mayorista%' then 1 else 0 end)   mayorista_f,
                 max(case when tier = 'top' then 1 else 0 end)             top_f,
                 max(case when tier = 'mid' then 1 else 0 end)             mid_f,
                 max(case when tier = 'basic_potential' then 1 else 0 end) basic_potential_f,
                 max(case when tier = 'basic' then 1 else 0 end)           basic_f,
                 case
                     when mayorista_f = 1 then 'mayorista'
                     when top_f = 1 then 'top'
                     when mid_f = 1 then 'mid'
                     when basic_potential_f = 1 then 'basic_potential'
                     else 'basic' end     as                               tier
          from FAVODATA.SNP_SANDBOX.AN_ENTREP_TIER_TEST t
                   left join FAVODATA.SNP_UNTRUSTED.MAYORISTAS m
                             on m.dynamo_leader_id = t.dynamo_leader_id and m.leader_class <> 'top_mayorista'
          group by 1)
        ,
     customer_tier
         as
         (select dynamo_customer_id,
                 case
                     when t.store_url = 'gruporomost' then 'gruporomo'
                     when m.leader_class = 'mayorista_test_st' then storeurl_origin
                     else t.store_url end as                        store_url_adj,
                 lt.tier                  as                        leader_tier,
                 lt.activation_date       as                        leader_activation_date,
                 min(t.activation_date)                             customer_activation_date,
                 max(case when t.tier = 'leader' then 1 else 0 end) leader_f,
                 max(case when t.tier = 'heavy' then 1 else 0 end)  heavy_f,
                 max(case when t.tier = 'mid' then 1 else 0 end)    mid_fa,
                 max(case when t.tier = 'low' then 1 else 0 end)    low_f,
                 case
                     when leader_f = 1 then 'leader'
                     when heavy_f = 1 then 'heavy'
                     when mid_fa = 1 then 'mid'
                     else 'low' end       as                        customer_tier
          from favodata.snp_sandbox.an_customer_tier_test t
                   left join FAVODATA.SNP_UNTRUSTED.MAYORISTAS m
                             on t.store_url = m.storeurl and m.leader_class <> 'top_mayorista'
                   left join leader_tier lt on lt.store_url = store_url_adj
          group by 1, 2, 3, 4),
     cohort_pre as
         (select case
                     when b.store_url = 'gruporomost' then 'gruporomo'
                     when m.leader_class = 'mayorista_test_st' then storeurl_origin
                     else b.store_url end                                                as store_url_adj,
                 t.tier,
                 case when t.tier = 'mayorista' then 'mayorista' else 'no mayorista' end as cluster,
                 date_trunc('week', t.activation_date)                                   as activation_week,
                 date_trunc('week', create_date_time_tz::date)                              calendar_week,
                 --weekofyear(t.activation_date) as activation_week,
                 --weekofyear(create_date_time_tz::date) as calendar_week,
                 datediff('week', activation_week, calendar_week)                           relative_week,
                 -- calendar_week - activation_week as relative_week,
                 sum(case
                         when coupon like any ('FAV-%', 'POC-FAV-%', 'MAY-%', 'POC-MAY-%') then net_value
                         else gross_value end)                                              gmv,
                 count(distinct order_number)                                               orders,
                 sum(quantity_fixed)                                                        units,
                 count(distinct b.dynamo_customer_id)                                       customers,
                 count(distinct case
                                    when ct.customer_tier <> 'leader' and date_trunc('week', t.activation_date) =
                                                                          date_trunc('week', create_date_time_tz::date)
                                        then b.dynamo_customer_id
                                    else null end)                                       as new_customers,
                 count(distinct case
                                    when ct.customer_tier <> 'leader' and date_trunc('week', t.activation_date) <>
                                                                          date_trunc('week', create_date_time_tz::date)
                                        then b.dynamo_customer_id
                                    else null end)                                       as old_customers,
                 count(distinct case
                                    when ct.customer_tier = 'leader'
                                        then b.dynamo_customer_id
                                    else null end)                                       as leader_customers
          from (select *,
                       case
                           when item_pack_sku is not null then
                               row_number() over (partition by order_number,item_pack_sku order by quantity)
                           else 1 end                                                           as n_row,
                       case when n_row = 1 then ifnull(item_pack_quantity, quantity) else 0 end as quantity_fixed
                from journey.base) b
                   left join FAVODATA.SNP_UNTRUSTED.MAYORISTAS m
                             on m.dynamo_leader_id = b.dynamo_leader_id and m.leader_class <> 'top_mayorista'
                   left join leader_tier t on t.store_url = store_url_adj
                   left join customer_tier ct on ct.dynamo_customer_id = b.dynamo_customer_id
          where b.country = 'PE'
            and b.order_status <> 'CANCEL'
            and b.cancel_date_time_tz is null
            and b.store_url not in (select store from FAVODATA.GROWTH_UNTRUSTED.FRAUDE where country = 'peru')
            and b.store_url not in (select store from FAVODATA.SNP_SANDBOX.CT_OCT_FRAUD)
            and relative_week > 0
          group by 1, 2, 3, 4, 5, 6) --;
        ,
     rank as (select store_url_adj,
                     activation_week,
                     relative_week,
                     gmv,
                     cluster,
                     sum(gmv)
                         over (partition by activation_week, relative_week,cluster order by gmv desc rows between unbounded preceding and current row) as cum_gmv,         --POR CLUSTER: MAYORISTA/NO MAYORISTA
                     sum(gmv)
                         over (partition by activation_week, relative_week order by gmv desc rows between unbounded preceding and current row)         as cum_gmv_total,   -- DE TODO FAVO
                     sum(gmv) over (partition by activation_week, relative_week,cluster)                                                               as gmv_total,       --POR CLUSTER
                     sum(gmv) over (partition by activation_week, relative_week)                                                                       as gmv_total_conso, -- DE TODO FAVO
                     div0(cum_gmv, gmv_total)                                                                                                          as percentile,
                     div0(cum_gmv_total, gmv_total_conso)                                                                                              as percentile_total
              from cohort_pre),
     cohort_post as
         (select c.*,
                 r.percentile,
                 r.percentile_total,
                 case
                     when r.percentile is null then 'churn_on_2nd_month'
                     when r.percentile <= 0.8 then 'top_80%_revenue'
                     else 'bottom_20%_revenue' end as pareto,
                 case
                     when r.percentile_total is null then 'churn_on_2nd_month'
                     when r.percentile_total <= 0.8 then 'top_80%_revenue'
                     else 'bottom_20%_revenue' end as pareto_total
          from cohort_pre c
                   left join rank r on r.store_url_adj = c.store_url_adj and r.activation_week = c.activation_week and
                                       r.relative_week = 1)
select activation_week,
       concat(weekofyear(activation_week), ' - ', year(activation_week)) as weeknum_year, -- concat
       relative_week,
       cluster,
       tier,
       pareto,
       pareto_total,
--count(distinct case when relative_month=0 then store_url_adj else null end) over (partition by activation_month,cluster,tier,pareto,pareto_total) as leaders_total,
       count(distinct store_url_adj)                                     as leaders,
--sum(case when relative_month=0 then gmv else 0 end) over (partition by activation_month,cluster,tier,pareto,pareto_total) as gmv_total,
       round(sum(gmv), 2)                                                as gmv,
       sum(orders)                                                       as orders,
       sum(units)                                                        as units,
       sum(customers)                                                    as customers,
       sum(new_customers)                                                as new_customers,
       sum(old_customers)                                                as old_customers,
       sum(leader_customers)                                             as leader_customers
from cohort_post
where calendar_week < date_trunc('week', current_date)
group by 1, 2, 3, 4, 5, 6, 7
    );

select * from SNP_SANDBOX.COHORTS_WEEKLY_LEADERS
where RELATIVE_WEEK < 0;