{{ config(materialized='table', schema='silver') }}

SELECT
    ws.ws_order_number                                          AS order_number,
    ws.ws_sold_date_sk                                          AS sold_date_sk,
    d.d_date                                                    AS sale_date,
    d.d_year                                                    AS sale_year,
    d.d_moy                                                     AS sale_month,
    d.d_day_name                                                AS day_of_week,
    -- Time dimension
    t.t_hour                                                    AS sale_hour,
    t.t_am_pm                                                   AS sale_am_pm,
    t.t_shift                                                   AS sale_shift,
    -- Customer
    ws.ws_bill_customer_sk                                      AS customer_sk,
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
    ws.ws_item_sk                                               AS item_sk,
    i.i_product_name                                            AS product_name,
    i.i_category                                                AS item_category,
    i.i_class                                                   AS item_class,
    i.i_brand                                                   AS item_brand,
    -- Web site & page
    ws.ws_web_site_sk                                           AS web_site_sk,
    wb.web_name                                                 AS web_site_name,
    wp.wp_type                                                  AS web_page_type,
    wp.wp_url                                                   AS web_page_url,
    -- Ship mode
    sm.sm_type                                                  AS ship_mode_type,
    sm.sm_carrier                                               AS ship_carrier,
    -- Promotion
    ws.ws_promo_sk                                              AS promo_sk,
    p.p_promo_name                                              AS promo_name,
    p.p_channel_email                                           AS promo_channel_email,
    p.p_channel_tv                                              AS promo_channel_tv,
    -- Metrics
    ws.ws_quantity                                              AS quantity,
    ws.ws_list_price                                            AS list_price,
    ws.ws_coupon_amt                                            AS coupon_discount,
    ws.ws_net_paid                                              AS net_paid,
    ws.ws_net_paid_inc_tax                                      AS net_paid_inc_tax,
    ws.ws_net_profit                                            AS net_profit,
    'web'                                                       AS channel

FROM {{ ref('bronze_web_sales') }} ws

LEFT JOIN delta.bronze.date_dim              d   ON ws.ws_sold_date_sk       = d.d_date_sk
LEFT JOIN delta.bronze.time_dim              t   ON ws.ws_sold_time_sk       = t.t_time_sk
LEFT JOIN delta.bronze.customer              c   ON ws.ws_bill_customer_sk   = c.c_customer_sk
LEFT JOIN delta.bronze.customer_address      ca  ON c.c_current_addr_sk      = ca.ca_address_sk
LEFT JOIN delta.bronze.customer_demographics cd  ON c.c_current_cdemo_sk    = cd.cd_demo_sk
LEFT JOIN delta.bronze.household_demographics hd ON c.c_current_hdemo_sk    = hd.hd_demo_sk
LEFT JOIN delta.bronze.income_band           ib  ON hd.hd_income_band_sk    = ib.ib_income_band_sk
LEFT JOIN delta.bronze.item                  i   ON ws.ws_item_sk            = i.i_item_sk
LEFT JOIN delta.bronze.web_site              wb  ON ws.ws_web_site_sk        = wb.web_site_sk
LEFT JOIN delta.bronze.web_page              wp  ON ws.ws_web_page_sk        = wp.wp_web_page_sk
LEFT JOIN delta.bronze.ship_mode             sm  ON ws.ws_ship_mode_sk       = sm.sm_ship_mode_sk
LEFT JOIN delta.bronze.promotion             p   ON ws.ws_promo_sk           = p.p_promo_sk

WHERE ws.ws_sold_date_sk IS NOT NULL
