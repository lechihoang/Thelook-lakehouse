{{
  config(
    tags=["data-quality", "gold", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for gold_customer_segments
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Country is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="country",
    description="Country cannot be null"
) }}

-- Test 2: Gender is valid
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="gender",
    value_set=['M', 'F', 'Other'],
    description="Gender must be valid"
) }}

-- Test 3: Age group is valid
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="age_group",
    value_set=['13-24', '25-34', '35-44', '45-54', '55-64', '65+'],
    description="Age group must be valid"
) }}

-- Test 4: Traffic source is valid
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="traffic_source",
    value_set=['Organic', 'Paid', 'Direct', 'Referral', 'Social', 'Email', 'Affiliate'],
    description="Traffic source must be valid"
) }}

-- Test 5: Total customers is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_customers",
    min_value=0,
    max_value=1000000,
    description="Total customers must be non-negative"
) }}

-- Test 6: Total revenue is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_revenue",
    min_value=0,
    max_value=100000000,
    description="Total revenue must be non-negative"
) }}

-- Test 7: Average order value is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="avg_order_value",
    min_value=0,
    max_value=100000,
    description="Average order value must be non-negative"
) }}

-- Test 8: Orders per customer is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="orders_per_customer",
    min_value=0,
    max_value=1000,
    description="Orders per customer must be non-negative"
) }}
