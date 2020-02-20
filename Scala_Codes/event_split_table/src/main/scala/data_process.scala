import java.util.Properties
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._

object data_process {
  def main(args: Array[String]) {
    //val csvFileEvent = "s3a://wanlinsinsightmeterdata/sampled_data.csv"

    val csvFileEvent = "s3a://wanlinsinsightmeterdata/SFMTA_Parking_Meter_Detailed_Revenue_Transactions.csv"
    val csvFileMeter = "s3a://wanlinsinsightmeterdata/Parking_Meters.csv"
    val spark = SparkSession.builder.appName("data_processo").config("spark.master", "local").getOrCreate()
    val df_Event = spark.read.format("csv").option("header", "true").load(csvFileEvent)
    val df_Meter = spark.read.format("csv")
      .option("header", "true")
      .load(csvFileMeter)
      .select("POST_ID", "CAP_COLOR", "LONGITUDE", "LATITUDE")
      .withColumn("latitude", col("LATITUDE").cast(FloatType))
      .withColumn("longitude", col("LONGITUDE").cast(FloatType))

    df_Meter.show()

    //val processCon: (String => String) = (arg: String) => {arg.substring(0,3)+((arg.substring(3,5).toInt / 15)*15).toString}
    val truncate_to_hours: (String => String) = (arg: String) => arg.split(":")(0)

    val sqlfunc = udf(truncate_to_hours)

    val processedEvents_pre = df_Event.withColumn("start_time", functions.to_timestamp(col("SESSION_START_DT"), "dd-MMM-yy hh.mm.ss a"))
      .withColumn("EventDate", functions.date_format(col("start_time"), "yyyy-MM-dd"))
      .withColumn("StartTime", functions.date_format(col("start_time"), "HH:mm:ss"))
      .withColumn("end_time", functions.to_timestamp(col("SESSION_END_DT"), "dd-MMM-yy hh.mm.ss a"))
      .withColumn("EndTime", functions.date_format(col("end_time"), "HH:mm:ss"))
      .drop("STREET_BLOCK").drop("PAYMENT_TYPE")
      .drop("SESSION_START_DT")
      .drop("SESSION_END_DT")
      .drop("METER_EVENT_TYPE")
      .drop("GROSS_PAID_AMT")
      .withColumn("Day_Of_Week", functions.date_format(col("EventDate"), "E"))
      .withColumn("StartWindow", sqlfunc(col("StartTime")))
      .withColumn("EndWindow", sqlfunc(col("EndTime")))

    val processedEvents = processedEvents_pre.drop(col("start_time")).drop(col("end_time"))

    processedEvents.show()


    val url = "jdbc:postgresql://10.0.0.10:5432/wanlin_db"

    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user", "wanlin")
    connectionProperties.setProperty("password", "1234")

    //dfp.write.option("header", "true").mode("append").jdbc(url, "Processed_Events", connectionProperties)
    val day = List("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    for (a <- day) {
      var dfp_split = processedEvents.filter(col("Day_Of_Week") === a)
      dfp_split.show()
      var dfp = dfp_split.join(df_Meter, "POST_ID")
      dfp.write.option("header", "true").mode("overwrite").jdbc(url, "processed_events_" + a, connectionProperties)
    }


    println("Job is done")

    spark.stop()
  }

}
