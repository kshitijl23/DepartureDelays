# why: tiny job to prove submit + worker execution
# why: hold Spark UI open long enough to inspect
# why: keep Spark UI up long enough to view on 4040
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("count_lines_ui").getOrCreate()
print("UI:", spark.sparkContext.uiWebUrl)   # prints actual 404x inside container
df = spark.range(0, 100_000_000)
print("Rows:", df.count())
time.sleep(120)  # keep UI alive for 2 minutes
spark.stop()