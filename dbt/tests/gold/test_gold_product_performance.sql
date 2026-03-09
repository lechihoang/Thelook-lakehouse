{{
  config(
    tags=["data-quality", "gold", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for gold_product_performance
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Product ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="product_id",
    description="Product ID cannot be null"
) }}

-- Test 2: Product name is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="product_name",
    description="Product name cannot be null"
) }}

-- Test 3: Product category is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="product_category",
    description="Product category cannot be null"
) }}

-- Test 4: Total revenue is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_revenue",
    min_value=0,
    max_value=10000000,
    description="Total revenue must be non-negative"
) }}

-- Test 5: Units sold is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="units_sold",
    min_value=0,
    max_value=100000,
    description="Units sold must be non-negative"
) }}

-- Test 6: Return rate is between 0 and 100
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="return_rate",
    min_value=0,
    max_value=100,
    description="Return rate must be between 0 and 100"
) }}

-- Test 7: Cancel rate is between 0 and 100
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="cancel_rate",
    min_value=0,
    max_value=100,
    description="Cancel rate must be between 0 and 100"
) }}

-- Test 8: Average selling price is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="avg_selling_price",
    min_value=0,
    max_value=100000,
    description="Average selling price must be non-negative"
) }}

-- Test 9: Total margin is non-negative
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="total_margin",
    min_value=0,
    max_value=10000000,
    description="Total margin must be non-negative"
) }}

-- Test 10: Return rate + cancel rate should not exceed 100
{{ dbt_utils.expression_is_true(
    expression="return_rate + cancel_rate <= 100",
    description="Return rate plus cancel rate cannot exceed 100%"
) }}
