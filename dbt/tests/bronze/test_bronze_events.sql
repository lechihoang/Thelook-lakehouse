{{
  config(
    tags=["data-quality", "bronze", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for bronze_events
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Row count within expected range
{{ dbt_expectations.expect_table_row_count_to_be_between(
    min_value=0,
    max_value=5000000,
    description="Events table should have reasonable row count"
) }}

-- Test 2: All IDs are unique
{{ dbt_expectations.expect_column_values_to_be_unique(
    column_name="id",
    description="Event ID must be unique"
) }}

-- Test 3: ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="id",
    description="Event ID cannot be null"
) }}

-- Test 4: Event type is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="event_type",
    description="Event type cannot be null"
) }}

-- Test 5: Event type is from allowed values
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="event_type",
    value_set=['view', 'add_to_cart', 'checkout', 'purchase', 'remove_from_cart', 'click', 'page_view'],
    description="Event type must be valid"
) }}

-- Test 6: User ID can be null (for anonymous events) but should be valid if present
{{ dbt_expectations.expect_column_values_to_be_of_type(
    column_name="user_id",
    column_type="INT",
    description="User ID must be integer when present"
) }}

-- Test 7: Session ID is not null (required for tracking)
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="session_id",
    description="Session ID cannot be null"
) }}

-- Test 8: Event timestamp is valid
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="created_at",
    description="Event timestamp cannot be null"
) }}

-- Test 9: Event type has reasonable cardinality
{{ dbt_expectations.expect_column_distinct_values_to_be_in_set(
    column_name="event_type",
    value_set=['view', 'add_to_cart', 'checkout', 'purchase', 'remove_from_cart', 'click', 'page_view'],
    description="Event type values must be from expected set"
) }}
