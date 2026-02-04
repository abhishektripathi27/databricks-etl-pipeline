from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

raw_path = "dbfs:/FileStore/raw/orders.csv"
silver_path = "dbfs:/FileStore/silver/orders_delta"

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("country", StringType(), True)
])

df = spark.read.format("csv") \
    .option("header", True) \
    .schema(schema) \
    .load(raw_path)

clean_df = df \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("amount").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("order_date").isNotNull()) \
    .filter(col("status").isNotNull())

clean_df.write.format("delta").mode("overwrite").save(silver_path)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS orders_silver
USING DELTA
LOCATION '{silver_path}'
""")

print("ETL Pipeline Completed Successfully!")
print(f"Raw input: {raw_path}")
print(f"Silver Delta output: {silver_path}")
