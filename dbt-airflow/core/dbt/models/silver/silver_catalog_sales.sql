{{ config(materialized='table', schema='silver') }}

SELECT
    cs.cs_order_number                                          AS order_number,
    cs.cs_sold_date_sk                                          AS sold_date_sk,
    d.d_date                                                    AS sale_date,
    d.d_year                                                    AS sale_year,
    d.d_moy                                                     AS sale_month,
    d.d_day_name                                                AS day_of_week,
    -- Time dimension
    t.t_hour                                                    AS sale_hour,
    t.t_am_pm                                                   AS sale_am_pm,
    t.t_shift                                                   AS sale_shift,
    -- Customer
    cs.cs_bill_customer_sk                                      AS customer_sk,
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
    cs.cs_item_sk                                               AS item_sk,
    i.i_product_name                                            AS product_name,
    i.i_category                                                AS item_category,
    i.i_class                                                   AS item_class,
    i.i_brand                                                   AS item_brand,
    -- Warehouse
    cs.cs_warehouse_sk                                          AS warehouse_sk,
    w.w_warehouse_name                                          AS warehouse_name,
    w.w_city                                                    AS warehouse_city,
    w.w_state                                                   AS warehouse_state,
    -- Catalog page
    cp.cp_department                                            AS catalog_department,
    cp.cp_catalog_number                                        AS catalog_number,
    cp.cp_type                                                  AS catalog_type,
    -- Call center
    cc.cc_name                                                  AS call_center_name,
    cc.cc_hours                                                 AS call_center_hours,
    cc.cc_manager                                               AS call_center_manager,
    -- Ship mode
    sm.sm_type                                                  AS ship_mode_type,
    sm.sm_carrier                                               AS ship_carrier,
    -- Promotion
    cs.cs_promo_sk                                              AS promo_sk,
    p.p_promo_name                                              AS promo_name,
    p.p_channel_email                                           AS promo_channel_email,
    p.p_channel_tv                                              AS promo_channel_tv,
    -- Metrics
    cs.cs_quantity                                              AS quantity,
    cs.cs_list_price                                            AS list_price,
    cs.cs_coupon_amt                                            AS coupon_discount,
    cs.cs_net_paid                                              AS net_paid,
    cs.cs_net_paid_inc_tax                                      AS net_paid_inc_tax,
    cs.cs_net_profit                                            AS net_profit,
    'catalog'                                                   AS channel

FROM {{ ref('bronze_catalog_sales') }} cs

LEFT JOIN delta.bronze.date_dim              d   ON cs.cs_sold_date_sk       = d.d_date_sk
LEFT JOIN delta.bronze.time_dim              t   ON cs.cs_sold_time_sk       = t.t_time_sk
LEFT JOIN delta.bronze.customer              c   ON cs.cs_bill_customer_sk   = c.c_customer_sk
LEFT JOIN delta.bronze.customer_address      ca  ON c.c_current_addr_sk      = ca.ca_address_sk
LEFT JOIN delta.bronze.customer_demographics cd  ON c.c_current_cdemo_sk    = cd.cd_demo_sk
LEFT JOIN delta.bronze.household_demographics hd ON c.c_current_hdemo_sk    = hd.hd_demo_sk
LEFT JOIN delta.bronze.income_band           ib  ON hd.hd_income_band_sk    = ib.ib_income_band_sk
LEFT JOIN delta.bronze.item                  i   ON cs.cs_item_sk            = i.i_item_sk
LEFT JOIN delta.bronze.warehouse             w   ON cs.cs_warehouse_sk       = w.w_warehouse_sk
LEFT JOIN delta.bronze.catalog_page          cp  ON cs.cs_catalog_page_sk    = cp.cp_catalog_page_sk
LEFT JOIN delta.bronze.call_center           cc  ON cs.cs_call_center_sk     = cc.cc_call_center_sk
LEFT JOIN delta.bronze.ship_mode             sm  ON cs.cs_ship_mode_sk       = sm.sm_ship_mode_sk
LEFT JOIN delta.bronze.promotion             p   ON cs.cs_promo_sk           = p.p_promo_sk

WHERE cs.cs_sold_date_sk IS NOT NULL
