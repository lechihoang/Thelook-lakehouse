{{ config(materialized='view', schema='bronze') }}

SELECT * FROM delta.bronze.web_sales
