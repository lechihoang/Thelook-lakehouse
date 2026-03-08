{{ config(materialized='table', schema='silver') }}

SELECT
    cr.cr_order_number                                          AS order_number,
    cr.cr_returned_date_sk                                      AS returned_date_sk,
    d.d_date                                                    AS return_date,
    d.d_year                                                    AS return_year,
    d.d_moy                                                     AS return_month,
    d.d_day_name                                                AS day_of_week,
    -- Time
    t.t_hour                                                    AS return_hour,
    t.t_am_pm                                                   AS return_am_pm,
    -- Customer (refunded)
    cr.cr_refunded_customer_sk                                  AS customer_sk,
    c.c_first_name || ' ' || c.c_last_name                     AS customer_name,
    cd.cd_gender                                                AS customer_gender,
    -- Customer address
    ca.ca_city                                                  AS customer_city,
    ca.ca_state                                                 AS customer_state,
    ca.ca_country                                               AS customer_country,
    -- Item
    cr.cr_item_sk                                               AS item_sk,
    i.i_product_name                                            AS product_name,
    i.i_category                                                AS item_category,
    i.i_class                                                   AS item_class,
    i.i_brand                                                   AS item_brand,
    -- Warehouse
    cr.cr_warehouse_sk                                          AS warehouse_sk,
    w.w_warehouse_name                                          AS warehouse_name,
    w.w_city                                                    AS warehouse_city,
    -- Call center
    cr.cr_call_center_sk                                        AS call_center_sk,
    cc.cc_name                                                  AS call_center_name,
    cc.cc_manager                                               AS call_center_manager,
    -- Ship mode
    cr.cr_ship_mode_sk                                          AS ship_mode_sk,
    sm.sm_type                                                  AS ship_mode_type,
    sm.sm_carrier                                               AS ship_carrier,
    -- Return reason
    cr.cr_reason_sk                                             AS reason_sk,
    r.r_reason_desc                                             AS return_reason,
    -- Metrics
    cr.cr_return_quantity                                       AS return_quantity,
    cr.cr_return_amt                                            AS return_amt,
    cr.cr_return_tax                                            AS return_tax,
    cr.cr_return_amt_inc_tax                                    AS return_amt_inc_tax,
    cr.cr_fee                                                   AS fee,
    cr.cr_return_ship_cost                                      AS ship_cost,
    cr.cr_refunded_cash                                         AS refunded_cash,
    cr.cr_reversed_charge                                       AS reversed_charge,
    cr.cr_store_credit                                          AS store_credit,
    cr.cr_net_loss                                              AS net_loss,
    'catalog'                                                   AS channel

FROM {{ ref('bronze_catalog_returns') }} cr

LEFT JOIN delta.bronze.date_dim              d   ON cr.cr_returned_date_sk      = d.d_date_sk
LEFT JOIN delta.bronze.time_dim              t   ON cr.cr_returned_time_sk      = t.t_time_sk
LEFT JOIN delta.bronze.customer              c   ON cr.cr_refunded_customer_sk  = c.c_customer_sk
LEFT JOIN delta.bronze.customer_address      ca  ON c.c_current_addr_sk         = ca.ca_address_sk
LEFT JOIN delta.bronze.customer_demographics cd  ON c.c_current_cdemo_sk       = cd.cd_demo_sk
LEFT JOIN delta.bronze.item                  i   ON cr.cr_item_sk               = i.i_item_sk
LEFT JOIN delta.bronze.warehouse             w   ON cr.cr_warehouse_sk          = w.w_warehouse_sk
LEFT JOIN delta.bronze.call_center           cc  ON cr.cr_call_center_sk        = cc.cc_call_center_sk
LEFT JOIN delta.bronze.ship_mode             sm  ON cr.cr_ship_mode_sk          = sm.sm_ship_mode_sk
LEFT JOIN delta.bronze.reason                r   ON cr.cr_reason_sk             = r.r_reason_sk

WHERE cr.cr_returned_date_sk IS NOT NULL
