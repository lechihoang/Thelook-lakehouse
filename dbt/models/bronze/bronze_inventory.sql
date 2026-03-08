{{ config(materialized='view', schema='bronze') }}

SELECT * FROM delta.bronze.inventory
