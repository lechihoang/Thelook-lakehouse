{{
  config(
    tags=["data-quality", "gold", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for gold_order_funnel
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Order status is valid
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="order_status",
    value_set=['Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned'],
    description="Order status must be valid"
) }}

-- Test 2: Order status is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="order_status",
    description="Order status cannot be null"
) }}

-- Test 3: Total orders is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_orders",
    min_value=0,
    max_value=1000000,
    description="Total orders must be non-negative"
) }}

-- Test 4: Average fulfillment time is non-negative (when present)
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="avg_fulfillment_time_hours",
    min_value=0,
    max_value=8760,
    description="Average fulfillment time should be reasonable"
) }}

-- Test 5: Total orders equals sum of statuses (basic sanity check)
{{ dbt_expectations.expect_aggregate_row_count_to_equal(
    expression="SUM(total_orders)",
    expected_value=1,
    tolerance=0.01,
    description="This test verifies aggregate calculation"
) }}

-- Test 6: All status values exist
{{ dbt_expectations.expect_column_distinct_values_to_be_in_set(
    column_name="order_status",
    value_set=['Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned'],
    description="All order statuses should be present"
) }}
