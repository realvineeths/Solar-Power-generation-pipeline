# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *

# spark = SparkSession.builder \
#     .appName("KafkaStream") \
#     .getOrCreate()

# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("subscribe", "4UPUqMRk7TRMgml, 81aHJ1q11NBPMrL, 9kRcWv60rDACzjR, Et9kgGMDl729KT4,IQ2d7wF4YD8zU1Q, LYwnQax7tkwH5Cb, LlT2YUhhzqhg5Sw, Mx2yZCDsyf6DPfv,NgDl19wMapZy17u, PeE6FRyGXUgsRhN, Qf4GUc1pJu5T6c6, Quc1TzYxW2pYoWX,V94E5Ben1TlhnDV, WcxssY2VbP4hApt, mqwcsP2rE7J0TFp, oZ35aAeoifZaQzV, oZZkBaNadn6DNKz, q49J1IKaHRwDQnt, rrq4fwE8jgrTyWY,xoJJ8DcxJEcupym") \
#   .option("numRows", 25) \
#   .option("startingOffsets", "latest") \
#   .load()

# spark.conf.set("spark.sql.shuffle.partitions", "500")

# # assuming the data has the same schema as the producer's
# schema = "PLANT_ID FLOAT, SOURCE_KEY STRING, DC_POWER FLOAT, AC_POWER FLOAT, DAILY_YIELD FLOAT, TOTAL_YIELD FLOAT"

# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#   .select(from_json("value", schema).alias("data")).select("data.*")

# # do some calculation on TOTAL_YIELD
# df = df.select("SOURCE_KEY", "TOTAL_YIELD", (col("TOTAL_YIELD") * 2).alias("TOTAL_YIELD_MULTIPLIED_BY_2"))

# # write the stream to console for debugging purposes
# query = df \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *

# spark = SparkSession.builder \
#     .appName("KafkaStream") \
#     .getOrCreate()

# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "4UPUqMRk7TRMgml, 81aHJ1q11NBPMrL, 9kRcWv60rDACzjR, Et9kgGMDl729KT4,IQ2d7wF4YD8zU1Q, LYwnQax7tkwH5Cb, LlT2YUhhzqhg5Sw, Mx2yZCDsyf6DPfv,NgDl19wMapZy17u, PeE6FRyGXUgsRhN, Qf4GUc1pJu5T6c6, Quc1TzYxW2pYoWX,V94E5Ben1TlhnDV, WcxssY2VbP4hApt, mqwcsP2rE7J0TFp, oZ35aAeoifZaQzV, oZZkBaNadn6DNKz, q49J1IKaHRwDQnt, rrq4fwE8jgrTyWY, vOuJvMaM2sgwLmb,xMbIugepa2P7lBB, xoJJ8DcxJEcupym") \
#     .option("startingOffsets", "latest") \
#     .load()

# # assuming the data has the same schema as the producer's
# schema = "PLANT_ID FLOAT, SOURCE_KEY STRING, DC_POWER FLOAT, AC_POWER FLOAT, DAILY_YIELD FLOAT, TOTAL_YIELD FLOAT"

# # select the necessary columns and add a timestamp column
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .select(from_json("value", schema).alias("data")).select("data.*") \
#     .withColumn("timestamp", current_timestamp())

# # group by topic and calculate the average of TOTAL_YIELD until the current timestamp
# df_avg = df.groupBy("SOURCE_KEY").agg(avg("TOTAL_YIELD").alias("AVG_TOTAL_YIELD"))

# # write the stream to console for debugging purposes
# query = df_avg \
#     .writeStream \
#     .format("console") \
#     .outputMode("complete") \
#     .start()

# query.awaitTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime

spark = SparkSession.builder \
    .appName("KafkaStream") \
    .getOrCreate()

start_time = datetime.now()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "4UPUqMRk7TRMgml, 81aHJ1q11NBPMrL, 9kRcWv60rDACzjR, Et9kgGMDl729KT4,IQ2d7wF4YD8zU1Q, LYwnQax7tkwH5Cb, LlT2YUhhzqhg5Sw, Mx2yZCDsyf6DPfv,NgDl19wMapZy17u, PeE6FRyGXUgsRhN, Qf4GUc1pJu5T6c6, Quc1TzYxW2pYoWX,V94E5Ben1TlhnDV, WcxssY2VbP4hApt, mqwcsP2rE7J0TFp, oZ35aAeoifZaQzV, oZZkBaNadn6DNKz, q49J1IKaHRwDQnt, rrq4fwE8jgrTyWY, vOuJvMaM2sgwLmb,xMbIugepa2P7lBB, xoJJ8DcxJEcupym") \
  .option("startingOffsets", "latest") \
  .load()


# spark.conf.set("spark.sql.shuffle.partitions", "500")

# assuming the data has the same schema as the producer's
schema = StructType([
    StructField("PLANT_ID", DoubleType(), True),
    StructField("SOURCE_KEY", StringType(), True),
    StructField("DC_POWER", DoubleType(), True),
    StructField("AC_POWER", DoubleType(), True),
    StructField("DAILY_YIELD", DoubleType(), True),
    StructField("TOTAL_YIELD", DoubleType(), True)
])

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .select(from_json("value", schema).alias("data")).select("data.*")

# do some calculation on TOTAL_YIELD
df = df.select("SOURCE_KEY", "TOTAL_YIELD", (col("TOTAL_YIELD") * 2).alias("TOTAL_YIELD_MULTIPLIED_BY_2"))

# write the stream to a MySQL database
mysql_url = "jdbc:mysql://localhost:3306/dbtproj"
mysql_properties = {
    "user": "root",
    "password": "rootpass",
    "driver": "com.mysql.jdbc.Driver"
}

query = df \
    .writeStream \
    .foreachBatch(lambda batchDF, batchId: batchDF.write.jdbc(mysql_url, "streamt", mode="append",properties=mysql_properties)) \
    .start()






query.awaitTermination()

end_time = datetime.now()
duration = end_time - start_time

print(f"Processing completed in {duration.total_seconds()} seconds.")




# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import TimestampType

# spark = SparkSession.builder \
#     .appName("KafkaStream") \
#     .getOrCreate()

# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "4UPUqMRk7TRMgml, 81aHJ1q11NBPMrL, 9kRcWv60rDACzjR, Et9kgGMDl729KT4,IQ2d7wF4YD8zU1Q, LYwnQax7tkwH5Cb, LlT2YUhhzqhg5Sw, Mx2yZCDsyf6DPfv,NgDl19wMapZy17u, PeE6FRyGXUgsRhN, Qf4GUc1pJu5T6c6, Quc1TzYxW2pYoWX,V94E5Ben1TlhnDV, WcxssY2VbP4hApt, mqwcsP2rE7J0TFp, oZ35aAeoifZaQzV, oZZkBaNadn6DNKz, q49J1IKaHRwDQnt, rrq4fwE8jgrTyWY, vOuJvMaM2sgwLmb,xMbIugepa2P7lBB, xoJJ8DcxJEcupym") \
#     .option("startingOffsets", "latest") \
#     .load()

# # assuming the data has the same schema as the producer's
# schema = "PLANT_ID FLOAT, SOURCE_KEY STRING, DC_POWER FLOAT, AC_POWER FLOAT, DAILY_YIELD FLOAT, TOTAL_YIELD FLOAT"

# # select the necessary columns and add a timestamp column
# df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .select(from_json("value", schema).alias("data")).select("data.*") \
#     .withColumn("timestamp", current_timestamp())

# # group by topic and calculate the average of TOTAL_YIELD until the current timestamp
# df_avg = df.groupBy("SOURCE_KEY").agg(avg("TOTAL_YIELD").alias("AVG_TOTAL_YIELD"), max("timestamp").alias("max_timestamp"))

# # write the stream to MySQL database
# url = "jdbc:mysql://localhost:3306/dbtproj"
# user = "root"
# password = "rootpass"
# table = "stremavg"

# query = df_avg \
#     .writeStream \
#     .foreachBatch(lambda df, epochId: df.write \
#         .jdbc(url=url, table=table, mode="append", properties={"user": user, "password": password})) \
#     .outputMode("update") \
#     .start()

# query.awaitTermination()
