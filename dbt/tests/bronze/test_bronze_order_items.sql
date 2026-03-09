{{
  config(
    tags=["data-quality", "bronze", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for bronze_order_items
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Row count within expected range
{{ dbt_expectations.expect_table_row_count_to_be_between(
    min_value=0,
    max_value=500000,
    description="Order items table should have reasonable row count"
) }}

-- Test 2: All IDs are unique
{{ dbt_expectations.expect_column_values_to_be_unique(
    column_name="id",
    description="Order item ID must be unique"
) }}

-- Test 3: ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="id",
    description="Order item ID cannot be null"
) }}

-- Test 4: Order ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="order_id",
    description="Order ID cannot be null"
) }}

-- Test 5: Product ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="product_id",
    description="Product ID cannot be null"
) }}

-- Test 6: Sale price is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="sale_price",
    description="Sale price cannot be null"
) }}

-- Test 7: Sale price is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="sale_price",
    min_value=0,
    max_value=100000,
    description="Sale price must be non-negative"
) }}

-- Test 8: Quantity is valid
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="quantity",
    min_value=1,
    max_value=100,
    description="Quantity must be between 1 and 100"
) }}

-- Test 9: ID is a valid integer
{{ dbt_expectations.expect_column_values_to_be_of_type(
    column_name="id",
    column_type="INT",
    description="Order item ID must be integer"
) }}
