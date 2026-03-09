{{
  config(
    tags=["data-quality", "bronze", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for bronze_orders
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Row count within expected range
{{ dbt_expectations.expect_table_row_count_to_be_between(
    min_value=0,
    max_value=1000000,
    description="Orders table should have reasonable row count"
) }}

-- Test 2: All IDs are unique
{{ dbt_expectations.expect_column_values_to_be_unique(
    column_name="id",
    description="Order ID must be unique"
) }}

-- Test 3: ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="id",
    description="Order ID cannot be null"
) }}

-- Test 4: User ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="user_id",
    description="User ID cannot be null"
) }}

-- Test 5: Status is from allowed values
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="status",
    value_set=['Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned'],
    description="Order status must be valid"
) }}

-- Test 6: Order date is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="created_at",
    description="Order created_at cannot be null"
) }}

-- Test 7: Order date is in the past or present
{{ dbt_expectations.expect_column_values_to_be_in_type_list(
    column_name="created_at",
    type_list=["TIMESTAMP", "DATE", "DATETIME"],
    description="created_at must be a timestamp type"
) }}

-- Test 8: ID is a valid integer
{{ dbt_expectations.expect_column_values_to_be_of_type(
    column_name="id",
    column_type="INT",
    description="Order ID must be integer"
) }}
