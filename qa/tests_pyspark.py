from pyspark.sql import SparkSession, functions as F

def _log(k, v):
    print(f"[QA] {k}: {v}")

spark = SparkSession.builder.appName("QA_DepartureDelays").getOrCreate()

# --- Source: CSV in the shared volume ---
src = (
    spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/opt/data/departuredelays.csv")
)

# Columns that match MySQL (DelayId and upload_dt are auto in MySQL)
cols = ["date", "delay", "distance", "origin", "destination"]
to_write = (
    src.select(*cols)
       .withColumn("date", F.col("date").cast("string"))   # MySQL column is VARCHAR
)

# --- JDBC connection to MySQL inside docker network ---
url  = "jdbc:mysql://departuredelay_mysql:3306/flightdb"
opts = {
    "user": "root",
    "password": "Root!234",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read target (may be empty but table exists)
tgt = spark.read.jdbc(url=url, table="flight_delays", properties=opts)

# --- Load once if empty, then re-read ---
if tgt.count() == 0:
    (to_write.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", "flight_delays")
        .option("driver", opts["driver"])
        .option("user", opts["user"])
        .option("password", opts["password"])
        .mode("append")
        .save())
    tgt = spark.read.jdbc(url=url, table="flight_delays", properties=opts)

# --- QA checks ---
_log("src_count", src.count())
_log("tgt_count", tgt.count())

# Duplicate keys on target (DelayId is AUTO_INCREMENT; should be 0)
_log("tgt_duplicate_keys", tgt.groupBy("DelayId").count().filter("count > 1").count())

# Business-key comparison
keys = ["date", "origin", "destination", "delay", "distance"]
src_minus_tgt = src.select(*keys).join(tgt.select(*keys), keys, "left_anti")
tgt_minus_src = tgt.select(*keys).join(src.select(*keys), keys, "left_anti")
_log("rows_src_minus_tgt", src_minus_tgt.count())
_log("rows_tgt_minus_src", tgt_minus_src.count())

spark.stop()