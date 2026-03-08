{{ config(materialized='table', schema='silver') }}

SELECT
    ss.ss_ticket_number                                         AS ticket_number,
    ss.ss_sold_date_sk                                          AS sold_date_sk,
    d.d_date                                                    AS sale_date,
    d.d_year                                                    AS sale_year,
    d.d_moy                                                     AS sale_month,
    d.d_day_name                                                AS day_of_week,
    -- Time dimension
    t.t_hour                                                    AS sale_hour,
    t.t_am_pm                                                   AS sale_am_pm,
    t.t_shift                                                   AS sale_shift,
    t.t_meal_time                                               AS sale_meal_time,
    -- Customer
    ss.ss_customer_sk                                           AS customer_sk,
    c.c_first_name || ' ' || c.c_last_name                     AS customer_name,
    cd.cd_gender                                                AS customer_gender,
    cd.cd_education_status                                      AS education_status,
    cd.cd_marital_status                                        AS marital_status,
    -- Customer address
    ca.ca_city                                                  AS customer_city,
    ca.ca_state                                                 AS customer_state,
    ca.ca_country                                               AS customer_country,
    ca.ca_zip                                                   AS customer_zip,
    -- Income band
    ib.ib_lower_bound                                           AS income_lower_bound,
    ib.ib_upper_bound                                           AS income_upper_bound,
    -- Item
    ss.ss_item_sk                                               AS item_sk,
    i.i_product_name                                            AS product_name,
    i.i_category                                                AS item_category,
    i.i_class                                                   AS item_class,
    i.i_brand                                                   AS item_brand,
    -- Store
    ss.ss_store_sk                                              AS store_sk,
    s.s_store_name                                              AS store_name,
    s.s_city                                                    AS store_city,
    s.s_state                                                   AS store_state,
    -- Promotion
    ss.ss_promo_sk                                              AS promo_sk,
    p.p_promo_name                                              AS promo_name,
    p.p_channel_email                                           AS promo_channel_email,
    p.p_channel_tv                                              AS promo_channel_tv,
    p.p_channel_radio                                           AS promo_channel_radio,
    -- Metrics
    ss.ss_quantity                                              AS quantity,
    ss.ss_list_price                                            AS list_price,
    ss.ss_coupon_amt                                            AS coupon_discount,
    ss.ss_net_paid                                              AS net_paid,
    ss.ss_net_paid_inc_tax                                      AS net_paid_inc_tax,
    ss.ss_net_profit                                            AS net_profit,
    'store'                                                     AS channel

FROM {{ ref('bronze_store_sales') }} ss

LEFT JOIN delta.bronze.date_dim              d   ON ss.ss_sold_date_sk      = d.d_date_sk
LEFT JOIN delta.bronze.time_dim              t   ON ss.ss_sold_time_sk      = t.t_time_sk
LEFT JOIN delta.bronze.customer              c   ON ss.ss_customer_sk       = c.c_customer_sk
LEFT JOIN delta.bronze.customer_address      ca  ON c.c_current_addr_sk     = ca.ca_address_sk
LEFT JOIN delta.bronze.customer_demographics cd  ON c.c_current_cdemo_sk   = cd.cd_demo_sk
LEFT JOIN delta.bronze.household_demographics hd ON c.c_current_hdemo_sk   = hd.hd_demo_sk
LEFT JOIN delta.bronze.income_band           ib  ON hd.hd_income_band_sk   = ib.ib_income_band_sk
LEFT JOIN delta.bronze.item                  i   ON ss.ss_item_sk           = i.i_item_sk
LEFT JOIN delta.bronze.store                 s   ON ss.ss_store_sk          = s.s_store_sk
LEFT JOIN delta.bronze.promotion             p   ON ss.ss_promo_sk          = p.p_promo_sk

WHERE ss.ss_sold_date_sk IS NOT NULL
