from pyspark.sql import SparkSession, functions as F

def _log(k, v): print(f"[QA] {k}: {v}", flush=True)

spark = SparkSession.builder.appName("QA_Flight_Delays").getOrCreate()

# --- Source: CSV from the shared /opt/data volume ---
src = (
    spark.read.format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load("/opt/data/departuredelays.csv")
         .select("date", "delay", "distance", "origin", "destination")
)

# --- Target: MySQL table inside the docker network ---
url  = "jdbc:mysql://departuredelay_mysql:3306/flightdb"
opts = {
    "user": "root",
    "password": "Root!234",
    "driver": "com.mysql.cj.jdbc.Driver"
}
tgt = spark.read.jdbc(url=url, table="flight_delays", properties=opts) \
             .select("date", "delay", "distance", "origin", "destination")

keys = ["date", "origin", "destination", "delay", "distance"]

# 1) Row counts
_log("src_count", src.count())
_log("tgt_count", tgt.count())

# 2) Duplicate keys on target
dups = (tgt.groupBy(*keys).count().filter("count > 1").count())
_log("tgt_duplicate_natural_keys", dups)

# 3) Source - Target
src_minus_tgt = src.select(*keys).join(tgt.select(*keys), keys, "left_anti")
_log("rows_src_minus_tgt", src_minus_tgt.count())

# 4) Target - Source 
tgt_minus_src = tgt.select(*keys).join(src.select(*keys), keys, "left_anti")
_log("rows_tgt_minus_src", tgt_minus_src.count())

spark.stop()