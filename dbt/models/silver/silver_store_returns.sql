{{ config(materialized='table', schema='silver') }}

SELECT
    sr.sr_ticket_number                                         AS ticket_number,
    sr.sr_returned_date_sk                                      AS returned_date_sk,
    d.d_date                                                    AS return_date,
    d.d_year                                                    AS return_year,
    d.d_moy                                                     AS return_month,
    d.d_day_name                                                AS day_of_week,
    -- Time
    t.t_hour                                                    AS return_hour,
    t.t_am_pm                                                   AS return_am_pm,
    -- Customer
    sr.sr_customer_sk                                           AS customer_sk,
    c.c_first_name || ' ' || c.c_last_name                     AS customer_name,
    cd.cd_gender                                                AS customer_gender,
    -- Customer address
    ca.ca_city                                                  AS customer_city,
    ca.ca_state                                                 AS customer_state,
    ca.ca_country                                               AS customer_country,
    -- Item
    sr.sr_item_sk                                               AS item_sk,
    i.i_product_name                                            AS product_name,
    i.i_category                                                AS item_category,
    i.i_class                                                   AS item_class,
    i.i_brand                                                   AS item_brand,
    -- Store
    sr.sr_store_sk                                              AS store_sk,
    s.s_store_name                                              AS store_name,
    s.s_city                                                    AS store_city,
    s.s_state                                                   AS store_state,
    -- Return reason
    sr.sr_reason_sk                                             AS reason_sk,
    r.r_reason_desc                                             AS return_reason,
    -- Metrics
    sr.sr_return_quantity                                       AS return_quantity,
    sr.sr_return_amt                                            AS return_amt,
    sr.sr_return_tax                                            AS return_tax,
    sr.sr_return_amt_inc_tax                                    AS return_amt_inc_tax,
    sr.sr_fee                                                   AS fee,
    sr.sr_return_ship_cost                                      AS ship_cost,
    sr.sr_refunded_cash                                         AS refunded_cash,
    sr.sr_reversed_charge                                       AS reversed_charge,
    sr.sr_store_credit                                          AS store_credit,
    sr.sr_net_loss                                              AS net_loss,
    'store'                                                     AS channel

FROM {{ ref('bronze_store_returns') }} sr

LEFT JOIN delta.bronze.date_dim              d   ON sr.sr_returned_date_sk  = d.d_date_sk
LEFT JOIN delta.bronze.time_dim              t   ON sr.sr_return_time_sk    = t.t_time_sk
LEFT JOIN delta.bronze.customer              c   ON sr.sr_customer_sk       = c.c_customer_sk
LEFT JOIN delta.bronze.customer_address      ca  ON c.c_current_addr_sk     = ca.ca_address_sk
LEFT JOIN delta.bronze.customer_demographics cd  ON c.c_current_cdemo_sk   = cd.cd_demo_sk
LEFT JOIN delta.bronze.item                  i   ON sr.sr_item_sk           = i.i_item_sk
LEFT JOIN delta.bronze.store                 s   ON sr.sr_store_sk          = s.s_store_sk
LEFT JOIN delta.bronze.reason                r   ON sr.sr_reason_sk         = r.r_reason_sk

WHERE sr.sr_returned_date_sk IS NOT NULL
