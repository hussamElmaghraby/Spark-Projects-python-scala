import org.apache.spark.sql.types._
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

}