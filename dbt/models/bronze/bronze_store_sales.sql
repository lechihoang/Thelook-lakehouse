{{ config(materialized='view', schema='bronze') }}

SELECT * FROM delta.bronze.store_sales
