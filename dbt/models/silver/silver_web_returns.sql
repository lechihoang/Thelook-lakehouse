{{ config(materialized='table', schema='silver') }}

SELECT
    wr.wr_order_number                                          AS order_number,
    wr.wr_returned_date_sk                                      AS returned_date_sk,
    d.d_date                                                    AS return_date,
    d.d_year                                                    AS return_year,
    d.d_moy                                                     AS return_month,
    d.d_day_name                                                AS day_of_week,
    -- Time
    t.t_hour                                                    AS return_hour,
    t.t_am_pm                                                   AS return_am_pm,
    -- Customer (refunded)
    wr.wr_refunded_customer_sk                                  AS customer_sk,
    c.c_first_name || ' ' || c.c_last_name                     AS customer_name,
    cd.cd_gender                                                AS customer_gender,
    -- Customer address
    ca.ca_city                                                  AS customer_city,
    ca.ca_state                                                 AS customer_state,
    ca.ca_country                                               AS customer_country,
    -- Item
    wr.wr_item_sk                                               AS item_sk,
    i.i_product_name                                            AS product_name,
    i.i_category                                                AS item_category,
    i.i_class                                                   AS item_class,
    i.i_brand                                                   AS item_brand,
    -- Web page
    wr.wr_web_page_sk                                           AS web_page_sk,
    wp.wp_type                                                  AS web_page_type,
    wp.wp_url                                                   AS web_page_url,
    -- Return reason
    wr.wr_reason_sk                                             AS reason_sk,
    r.r_reason_desc                                             AS return_reason,
    -- Metrics
    wr.wr_return_quantity                                       AS return_quantity,
    wr.wr_return_amt                                            AS return_amt,
    wr.wr_return_tax                                            AS return_tax,
    wr.wr_return_amt_inc_tax                                    AS return_amt_inc_tax,
    wr.wr_fee                                                   AS fee,
    wr.wr_return_ship_cost                                      AS ship_cost,
    wr.wr_refunded_cash                                         AS refunded_cash,
    wr.wr_reversed_charge                                       AS reversed_charge,
    wr.wr_account_credit                                        AS account_credit,
    wr.wr_net_loss                                              AS net_loss,
    'web'                                                       AS channel

FROM {{ ref('bronze_web_returns') }} wr

LEFT JOIN delta.bronze.date_dim              d   ON wr.wr_returned_date_sk      = d.d_date_sk
LEFT JOIN delta.bronze.time_dim              t   ON wr.wr_returned_time_sk      = t.t_time_sk
LEFT JOIN delta.bronze.customer              c   ON wr.wr_refunded_customer_sk  = c.c_customer_sk
LEFT JOIN delta.bronze.customer_address      ca  ON c.c_current_addr_sk         = ca.ca_address_sk
LEFT JOIN delta.bronze.customer_demographics cd  ON c.c_current_cdemo_sk       = cd.cd_demo_sk
LEFT JOIN delta.bronze.item                  i   ON wr.wr_item_sk               = i.i_item_sk
LEFT JOIN delta.bronze.web_page              wp  ON wr.wr_web_page_sk           = wp.wp_web_page_sk
LEFT JOIN delta.bronze.reason                r   ON wr.wr_reason_sk             = r.r_reason_sk

WHERE wr.wr_returned_date_sk IS NOT NULL
