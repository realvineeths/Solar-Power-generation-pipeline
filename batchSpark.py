from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime


spark = SparkSession.builder \
    .appName("BatchQuery") \
    .getOrCreate()

# read data from the MySQL database
mysql_url = "jdbc:mysql://localhost:3306/dbtproj"
mysql_properties = {
    "user": "root",
    "password": "rootpass",
    "driver": "com.mysql.jdbc.Driver"
}

start_time = datetime.now()

df = spark \
    .read \
    .jdbc(mysql_url, "streamt2", properties=mysql_properties)

# do some calculation on TOTAL_YIELD
df = df.select("SOURCE_KEY", "TOTAL_YIELD", (col("TOTAL_YIELD") * 3).alias("TOTAL_YIELD_MULTIPLIED_BY_3"))

df.write.jdbc(mysql_url, "batcht3", mode="overwrite", properties=mysql_properties)

end_time = datetime.now()
duration = end_time - start_time

print(f"Processing completed in {duration.total_seconds()} seconds.")
