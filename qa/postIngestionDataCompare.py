from pyspark.sql import SparkSession, functions as F
from datetime import datetime

def _log(k, v): print(f"[QA] {k}: {v}", flush=True)

spark = SparkSession.builder.appName("QA_Flight_Delays").getOrCreate()

# --- Source: CSV ---
src = (
    spark.read.format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load("/opt/data/departuredelays.csv")
         .select("date", "delay", "distance", "origin", "destination")
)

# --- Target: MySQL (inside docker network) ---
url  = "jdbc:mysql://departuredelay_mysql:3306/flightdb"
opts = {"user": "root", "password": "Root!234", "driver": "com.mysql.cj.jdbc.Driver"}
tgt  = spark.read.jdbc(url=url, table="flight_delays", properties=opts) \
              .select("date", "delay", "distance", "origin", "destination")

keys = ["date", "origin", "destination", "delay", "distance"]

# 1) Row counts
src_cnt = src.count(); _log("src_count", src_cnt)
tgt_cnt = tgt.count(); _log("tgt_count", tgt_cnt)

# 2) Duplicate natural keys on target
dups = tgt.groupBy(*keys).count().filter("count > 1").count()
_log("tgt_duplicate_natural_keys", dups)

# 3) Source - Target
src_minus_tgt = src.select(*keys).join(tgt.select(*keys), keys, "left_anti")
smt_cnt = src_minus_tgt.count(); _log("rows_src_minus_tgt", smt_cnt)

# 4) Target - Source
tgt_minus_src = tgt.select(*keys).join(src.select(*keys), keys, "left_anti")
tms_cnt = tgt_minus_src.count(); _log("rows_tgt_minus_src", tms_cnt)

# --- Save a machine-readable summary for the repo ---
summary = [{
    "ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    "src_count": src_cnt,
    "tgt_count": tgt_cnt,
    "tgt_duplicate_natural_keys": dups,
    "rows_src_minus_tgt": smt_cnt,
    "rows_tgt_minus_src": tms_cnt
}]

# write results (1 file each)
summary_df = spark.createDataFrame(summary)
summary_df.coalesce(1).write.mode("overwrite").json("/opt/qa/_last_qa_results_json")
summary_df.coalesce(1).write.mode("overwrite").csv("/opt/qa/_last_qa_results_csv", header=True)

spark.stop()