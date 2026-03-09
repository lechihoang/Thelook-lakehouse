{{
  config(
    tags=["data-quality", "silver", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for silver_orders
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Row count within expected range
{{ dbt_expectations.expect_table_row_count_to_be_between(
    min_value=0,
    max_value=1000000,
    description="Enriched orders should have reasonable row count"
) }}

-- Test 2: Order ID is unique
{{ dbt_expectations.expect_column_values_to_be_unique(
    column_name="order_id",
    description="Order ID must be unique"
) }}

-- Test 3: Order ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="order_id",
    description="Order ID cannot be null"
) }}

-- Test 4: User ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="user_id",
    description="User ID cannot be null"
) }}

-- Test 5: Order status is valid
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="order_status",
    value_set=['Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned'],
    description="Order status must be valid"
) }}

-- Test 6: Order status is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="order_status",
    description="Order status cannot be null"
) }}

-- Test 7: Created at is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="created_at",
    description="Order created_at cannot be null"
) }}

-- Test 8: Customer name is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="customer_name",
    description="Customer name cannot be null"
) }}

-- Test 9: Customer country is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="customer_country",
    description="Customer country cannot be null"
) }}

-- Test 10: Traffic source is valid
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="traffic_source",
    value_set=['Organic', 'Paid', 'Direct', 'Referral', 'Social', 'Email', 'Affiliate'],
    description="Traffic source should be from expected set"
) }}

-- Test 11: Num of items is positive
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="num_of_items",
    min_value=1,
    max_value=100,
    description="Number of items must be at least 1"
) }}

-- Test 12: Age is within valid range
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="customer_age",
    min_value=13,
    max_value=120,
    description="Customer age should be between 13 and 120"
) }}
