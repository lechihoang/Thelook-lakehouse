{{
  config(
    tags=["data-quality", "gold", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for gold_sales_by_category
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Category is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="product_category",
    description="Product category cannot be null"
) }}

-- Test 2: Department is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="product_department",
    description="Product department cannot be null"
) }}

-- Test 3: Total orders is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_orders",
    min_value=0,
    max_value=1000000,
    description="Total orders must be non-negative"
) }}

-- Test 4: Total items sold is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_items_sold",
    min_value=0,
    max_value=1000000,
    description="Total items sold must be non-negative"
) }}

-- Test 5: Total revenue is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_revenue",
    min_value=0,
    max_value=100000000,
    description="Total revenue must be non-negative"
) }}

-- Test 6: Total gross margin is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_gross_margin",
    min_value=0,
    max_value=100000000,
    description="Total gross margin must be non-negative"
) }}

-- Test 7: Margin percentage is between 0 and 100
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="margin_pct",
    min_value=0,
    max_value=100,
    description="Margin percentage must be between 0 and 100"
) }}

-- Test 8: Average sale price is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="avg_sale_price",
    min_value=0,
    max_value=100000,
    description="Average sale price must be non-negative"
) }}

-- Test 9: Unique customers is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="unique_customers",
    min_value=0,
    max_value=100000,
    description="Unique customers must be non-negative"
) }}

-- Test 10: At least one row exists if there are sales
{{ dbt_utils.expression_is_true(
    expression="total_orders > 0",
    description="If category exists, should have at least one order"
) }}
