{{
  config(
    tags=["data-quality", "silver", "great-expectations"],
    severity = "error"
  )
}}

{# 
  Great Expectations tests for silver_events
  Uses dbt_expectations package (calogica/dbt_expectations)
#}

-- Test 1: Row count within expected range
{{ dbt_expectations.expect_table_row_count_to_be_between(
    min_value=0,
    max_value=5000000,
    description="Enriched events should have reasonable row count"
) }}

-- Test 2: Event ID is unique
{{ dbt_expectations.expect_column_values_to_be_unique(
    column_name="event_id",
    description="Event ID must be unique"
) }}

-- Test 3: Event ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="event_id",
    description="Event ID cannot be null"
) }}

-- Test 4: Session ID is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="session_id",
    description="Session ID cannot be null"
) }}

-- Test 5: Event type is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="event_type",
    description="Event type cannot be null"
) }}

-- Test 6: Event type is valid
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="event_type",
    value_set=['view', 'add_to_cart', 'checkout', 'purchase', 'remove_from_cart', 'click', 'page_view'],
    description="Event type must be valid"
) }}

-- Test 7: Event time is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="event_time",
    description="Event time cannot be null"
) }}

-- Test 8: Sequence number is valid
{{ dbt_expectations.expect_column_values_to_be_between(
    column_name="sequence_number",
    min_value=1,
    max_value=10000,
    description="Sequence number should be positive"
) }}

-- Test 9: Traffic source is valid (when present)
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="traffic_source",
    value_set=['Organic', 'Paid', 'Direct', 'Referral', 'Social', 'Email', 'Affiliate'],
    description="Traffic source should be from expected set"
) }}

-- Test 10: Browser is not null
{{ dbt_expectations.expect_column_values_to_not_be_null(
    column_name="browser",
    description="Browser cannot be null"
) }}

-- Test 11: is_ghost flag is boolean
{{ dbt_expectations.expect_column_values_to_be_in_set(
    column_name="is_ghost",
    value_set=[True, False],
    description="is_ghost should be boolean"
) }}

-- Test 12: Ghost events should have null user_id
{{ dbt_utils.expression_is_true(
    expression="CASE WHEN is_ghost = TRUE THEN user_id IS NULL ELSE TRUE END",
    description="Ghost events must have null user_id"
) }}
