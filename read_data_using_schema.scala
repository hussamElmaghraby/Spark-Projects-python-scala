import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}

object Reading_file {

val fireSchema = StructType(
                    Array(
                         StructField("CallNumber", IntegerType, true),
                         StructField("UnitID", StringType, true),
                         StructField("IncidentNumber", IntegerType, true),
                         StructField("CallType", StringType, true),
                         StructField("CallDate", StringType, true),
                         StructField("WatchDate", StringType, true),
                         StructField("CallFinalDisposition", StringType, true),
                         StructField("AvailableDtTm", StringType, true),
                         StructField("Address", StringType, true),
                         StructField("City", StringType, true),
                         StructField("Zipcode", IntegerType, true),
                         StructField("Battalion", StringType, true),
                         StructField("StationArea", StringType, true),
                         StructField("Box", StringType, true),
                         StructField("OriginalPriority", StringType, true),
                         StructField("Priority", StringType, true),
                         StructField("FinalPriority", IntegerType, true),
                         StructField("ALSUnit", BooleanType, true),
                         StructField("CallTypeGroup", StringType, true),
                         StructField("NumAlarms", IntegerType, true),
                         StructField("UnitType", StringType, true),
                         StructField("UnitSequenceInCallDispatch", IntegerType, true),
                         StructField("FirePreventionDistrict", StringType, true),
                         StructField("SupervisorDistrict", StringType, true),
                         StructField("Neighborhood", StringType, true),
                         StructField("Location", StringType, true),
                         StructField("RowID", StringType, true),
                         StructField("Delay", FloatType, true)
                    )
                        )
val fireCalls = "../data/fire-calls.csv"
val fireDF = spark.read.schema(fireSchema).option("header" , true).csv(fireCalls)
fireDF.show()

// ------------------------------------------------------------------------
//                       Saving and in many formats..
// Save it as parquet
val parquetPath = "any path"
fireDF.write.format("parquet").save(parquetPath)

// Save it as Parquet Table
val parquetTable = "any path"
fireDF.write.format("parquet").saveAsTable(parquetTable)
// ------------------------------------------------------------------------
//                  Performing transformations and actions
val fewFireDF = fireDF.select("CallNumber" , "UnitID" , "IncidentNumber").where($"CallType" =!= "Medical Incident")
fewFireDF.show(5 , false)

//show distinct call types
fireDF.select("CallType").where(col("CallType").isNotNull).agg(countDistinct('CallType) as 'DistinctCallTypes).show()

//filter for only distinct non-null CallTypes from all the rows
fireDF.select("CallType").where(col("CallType").isNotNull).distinct().show(10, false)

//rename  Delay column..
val newFireDF = fireDF.withColumnRenamed("Delay" , "ResposneDelayedinMins")

// select delays longer than 5 
newFireDF.select("ResposneDelayedinMins").where($"ResposneDelayedinMins" > 10).show()

val fireTsDF = newFireDF.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")).drop("CallDate")
.withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")).drop("WatchDate")
.withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),"MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm")
// Select the converted columns
fireTsDF.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show()
fireTsDF.select(year($"IncidentDate")).distinct().orderBy(year($"IncidentDate")).show()

// the most common types of fire calls
fireTsDF
.select("CallType")
.where(col("CallType").isNotNull)
.groupBy("CallType")
.count()
.orderBy(desc("count"))
.show(10, false)

}