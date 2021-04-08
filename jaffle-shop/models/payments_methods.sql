with
payments as (
  select * from {{ ref('stg_stripe__payments') }}
)
select 
  -- check -- same thing over and over
  -- check -- what if we have new payment methods? Not scalable.
  -- check -- comma issue
  order_id,
  {% set payment_methods = dbt_utils.get_column_values(table=ref('stg_stripe__payments'), column='payment_method') %}
  {%- for method in payment_methods %}
  sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount
  {%- if not loop.last %},{% endif %}
  {%- endfor %}
  -- sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
  -- sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
  -- sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount,
  -- sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount
from payments
group by 1