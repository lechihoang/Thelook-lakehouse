-- Create dedicated Debezium replication user
CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium123';
GRANT CONNECT ON DATABASE tpcds TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Create replication publication for fact tables
-- (run after TPC-DS schema is loaded)
-- CREATE PUBLICATION debezium_tpcds_pub FOR TABLE
--   store_sales, store_returns,
--   web_sales, web_returns,
--   catalog_sales, catalog_returns,
--   inventory;
