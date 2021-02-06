from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

fire_schema = StructType([
    StructField("CallNumber", IntegerType(), True),
    StructField("UnitID", StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', IntegerType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)
])


# Use the dataFrameReader ..
sf_fire_file = "../Projects/Reading_from_files_using_schema/data/fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
fire_df.show()

# ----------------------------------------------------------------------------
#                      Saving and writing in many formats
# Save it as parquet
parquet_path = "any path"
fire_df.write.format("parquet").save(parquet_path)

# Save it as Parquet Table
parquet_table = "any path"
fire_df.write.format("parquet").saveAsTable(parquet_table

# ----------------------------------------------------------------------------
#               Performing transformations and actions
few_frie_df=(fire_df.select("CallNumber", "UnitID", "IncidentNumber").where(
    col("CallType") != "Medical Incident"))

few_frie_df.show(5, truncate=False)

(fire_df
.select("CallType").where(col("CallType").isNotNull())
.alias("DistinctCallTypes").agg(countDistinct("CallType")).show())

# filter for only distinct non-null CallTypes from all the rows
(fire_df
.select("CallType")
.where(col("CallType").isNotNull()).distinct().show(10, False))


new_fire_df=fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df.select("ResponseDelayedinMins").where(
    col("ResponseDelayedinMins") > 5).show(5, False))

# Convert to more usable formats
fire_ts_df=(new_fire_df
.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
.drop("CallDate")
.withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
.drop("WatchDate")
.withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
"MM/dd/yyyy hh:mm:ss a"))
.drop("AvailableDtTm"))
# Select the converted columns
fire_ts_df.select("IncidentDate", "OnWatchDate",
                  "AvailableDtTS").show(5, False)
fire_ts_df.select(year('IncidentDate')).distinct().orderBy(
    year("IncidentDate")).show(10, False)

# the most common types of fire calls
(fire_ts_df.select("CallType")
.where(col("CallType").isNotNull()).groupBy("CallType")
.count().orderBy("count", ascending=False)
.show(n=10, truncate=False))

# some Computatinos
(fire_ts_df
.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
.show())
# ----------------------------------------------------------------------------
