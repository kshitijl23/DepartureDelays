from pyspark.sql import SparkSession, functions as F
import sys, traceback

def step(name):
    print(f"\n🟦 STEP: {name} ...", flush=True)

def ok(msg="OK"):
    print(f"✅ {msg}", flush=True)

def fail(e):
    print("❌ FAILED", flush=True)
    traceback.print_exc()
    # Exit non-zero so your CI/terminal clearly shows failure
    sys.exit(1)

try:
    step("Create Spark session")
    spark = SparkSession.builder.appName("DepartureDelays_Load").getOrCreate()
    print(f"ℹ️  Spark UI: {spark.sparkContext.uiWebUrl}", flush=True)
    ok("Spark session up")

    step("Read CSV from /opt/data/departuredelays.csv")
    df = (spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/opt/data/departuredelays.csv"))
    print(f"ℹ️  Input schema:\n{df.printSchema()}")
    cnt_src = df.count()
    print(f"ℹ️  Source row count: {cnt_src}")
    ok("CSV loaded")

    step("Add surrogate key (DelayId) + upload timestamp (upload_dt)")
    df_enh = (df
        .withColumn("DelayId", F.monotonically_increasing_id().cast("bigint"))
        .withColumn("upload_dt", F.current_timestamp()))
    cnt_enh = df_enh.count()
    print(f"ℹ️  Enhanced row count: {cnt_enh}")
    ok("Columns added")

    step("Sanity peek (top delays & SFO→ORD >120m)")
    df_enh.select("origin","destination","delay","distance") \
          .orderBy(F.col("delay").desc()) \
          .show(10, truncate=False)
    df_enh.filter((F.col("origin")=="SFO") &
                  (F.col("destination")=="ORD") &
                  (F.col("delay")>120)) \
          .select("date","delay","origin","destination") \
          .orderBy(F.col("delay").desc()) \
          .show(10, truncate=False)
    ok("Sanity peek done")

    step("Write to MySQL (flightdb.flight_delays)")
    url   = "jdbc:mysql://departuredelay_mysql:3306/flightdb"
    props = {"user":"root", "password":"Root!234", "driver":"com.mysql.cj.jdbc.Driver"}
    (df_enh
      .select("DelayId","date","delay","distance","origin","destination","upload_dt")
      .write.mode("overwrite")   # swap to 'append' after first run
      .jdbc(url=url, table="flight_delays", properties=props))
    ok("Write completed")

    step("Post-write verification (read back 5 rows via JDBC)")
    df_check = spark.read.jdbc(url=url, table="flight_delays", properties=props)
    print(f"ℹ️  Target row count (quick): {df_check.count()}")
    df_check.orderBy(F.col("DelayId").asc()).show(5, truncate=False)
    ok("Verified target")

    step("Stop Spark")
    spark.stop()
    ok("All done 🎉")

except Exception as e:
    fail(e)