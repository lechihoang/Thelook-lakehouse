from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, IntegerType,
)

def _debezium(after_schema: StructType) -> StructType:
    return StructType([
        StructField("op",    StringType()),
        StructField("ts_ms", LongType()),
        StructField("after", after_schema),
    ])

users = _debezium(StructType([
    StructField("id",             StringType()),
    StructField("first_name",     StringType()),
    StructField("last_name",      StringType()),
    StructField("email",          StringType()),
    StructField("age",            IntegerType()),
    StructField("gender",         StringType()),
    StructField("street_address", StringType()),
    StructField("postal_code",    StringType()),
    StructField("city",           StringType()),
    StructField("state",          StringType()),
    StructField("country",        StringType()),
    StructField("latitude",       DoubleType()),
    StructField("longitude",      DoubleType()),
    StructField("traffic_source", StringType()),
    StructField("created_at",     StringType()),
    StructField("updated_at",     StringType()),
]))

orders = _debezium(StructType([
    StructField("id",           StringType()),
    StructField("user_id",      StringType()),
    StructField("status",       StringType()),
    StructField("num_of_items", IntegerType()),
    StructField("created_at",   StringType()),
    StructField("updated_at",   StringType()),
    StructField("returned_at",  StringType()),
    StructField("shipped_at",   StringType()),
    StructField("delivered_at", StringType()),
    StructField("cancelled_at", StringType()),
]))

order_items = _debezium(StructType([
    StructField("id",           StringType()),
    StructField("order_id",     StringType()),
    StructField("product_id",   LongType()),
    StructField("status",       StringType()),
    StructField("quantity",     IntegerType()),
    StructField("sale_price",   DoubleType()),
    StructField("created_at",   StringType()),
    StructField("updated_at",   StringType()),
    StructField("shipped_at",   StringType()),
    StructField("delivered_at", StringType()),
    StructField("returned_at",  StringType()),
    StructField("cancelled_at", StringType()),
]))

events = _debezium(StructType([
    StructField("id",              StringType()),
    StructField("user_id",         StringType()),
    StructField("sequence_number", IntegerType()),
    StructField("session_id",      StringType()),
    StructField("ip_address",      StringType()),
    StructField("city",            StringType()),
    StructField("state",           StringType()),
    StructField("postal_code",     StringType()),
    StructField("browser",         StringType()),
    StructField("traffic_source",  StringType()),
    StructField("uri",             StringType()),
    StructField("event_type",      StringType()),
    StructField("created_at",      StringType()),
]))

products = _debezium(StructType([
    StructField("id",                     LongType()),
    StructField("cost",                   DoubleType()),
    StructField("category",               StringType()),
    StructField("name",                   StringType()),
    StructField("brand",                  StringType()),
    StructField("retail_price",           DoubleType()),
    StructField("department",             StringType()),
    StructField("sku",                    StringType()),
    StructField("distribution_center_id", LongType()),
]))

dist_centers = _debezium(StructType([
    StructField("id",        LongType()),
    StructField("name",      StringType()),
    StructField("latitude",  DoubleType()),
    StructField("longitude", DoubleType()),
]))

TOPIC_CONFIG = {
    "tpcds.public.users":        (users,        "users"),
    "tpcds.public.orders":       (orders,       "orders"),
    "tpcds.public.order_items":  (order_items,  "order_items"),
    "tpcds.public.events":       (events,       "events"),
    "tpcds.public.products":     (products,     "products"),
    "tpcds.public.dist_centers": (dist_centers, "dist_centers"),
}
