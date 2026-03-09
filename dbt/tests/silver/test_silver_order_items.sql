{{
  config(
    tags=["data-quality", "silver", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for silver_order_items
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Row count within expected range
{{ dbt_expectations.expect_table_row_count_to_be_between(
    min_value=0,
    max_value=500000,
    description="Enriched order items should have reasonable row count"
) }}

-- Test 2: Order item ID is unique
{{ dbt_expectations.expect_column_values_to_be_unique(
    column_name="order_item_id",
    description="Order item ID must be unique"
) }}

-- Test 3: Order item ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="order_item_id",
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

-- Test 6: Revenue is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="revenue",
    description="Revenue cannot be null"
) }}

-- Test 7: Revenue is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="revenue",
    min_value=0,
    max_value=100000,
    description="Revenue must be non-negative"
) }}

-- Test 8: Gross margin is within reasonable range
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="gross_margin",
    min_value=-10000,
    max_value=100000,
    description="Gross margin should be reasonable"
) }}

-- Test 9: Quantity is valid
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="quantity",
    min_value=1,
    max_value=100,
    description="Quantity must be between 1 and 100"
) }}

-- Test 10: Order status is valid
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="order_status",
    value_set=['Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned'],
    description="Order status must be valid"
) }}

-- Test 11: Customer country is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="customer_country",
    description="Customer country cannot be null"
) }}

-- Test 12: Distribution center is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="distribution_center",
    description="Distribution center cannot be null"
) }}

-- Test 13: Product category is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="product_category",
    description="Product category cannot be null"
) }}

-- Test 14: User ID is not null (all orders should have a user)
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="user_id",
    description="User ID cannot be null"
) }}
