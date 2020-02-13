import java.util.Properties

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions}
package my_Event
import
object event_processor {
    def main(args: Array[String]) {
      val t0 = System.nanoTime() //start time
      //val csvFileEvent = "s3a://wanlinsinsightmeterdata/sampled_data.csv"


      val csvFileEvent = "s3a://wanlinsinsightmeterdata/SFMTA_Parking_Meter_Detailed_Revenue_Transactions.csv"
      val csvFileMeter = "s3a://wanlinsinsightmeterdata/Parking_Meters.csv"

      val spark = SparkSession.builder.appName("data_processo").getOrCreate()

      /*
      val customSchemaEvent = StructType(Array(StructField("POST_ID", StringType, false),
        StructField("STREET_BLOCK", StringType, true),
        StructField("PAYMENT_TYPE", StringType, true),
        StructField("SESSION_START_DT", StringType, true),
        StructField("SESSION_END_DT", StringType, true),
        StructField("METER_EVENT_TYPE", StringType, true),
        StructField("GROSS_PAID_AMT", StringType, true)))


      val df_Event = spark.read.format("csv").option("header", "false").schema(customSchemaEvent).load(csvFileEvent)
      */


      val df_Event = spark.read.format("csv").option("header", "true").load(csvFileEvent)
      df_Event.show()

      val df_Meter = spark.read.format("csv")
        .option("header", "true")
        .load(csvFileMeter)
        .select("POST_ID", "CAP_COLOR", "LONGITUDE", "LATITUDE")
        .withColumn("latitude", col("LATITUDE").cast(FloatType))
        .withColumn("longitude", col("LONGITUDE").cast(FloatType))

      df_Meter.show()

      val t1 = System.nanoTime() //data_injest

      //val processCon: (String => String) = (arg: String) => {arg.substring(0,3)+((arg.substring(3,5).toInt / 15)*15).toString}
      val processCon: (String => String) = (arg: String) =>
        if (arg != null && arg.substring(3, 5).toInt / 15 != 0) arg.substring(0, 3) + ((arg.substring(3, 5).toInt / 15) * 15).toString
        else if (arg != null && arg.substring(3, 5).toInt / 15 == 0) (arg.substring(0, 3) + "00").toString
        else ""

      val sqlfunc = udf(processCon)

      val processedEvents = df_Event.withColumn("start_time", functions.to_timestamp(col("SESSION_START_DT"), "dd-MMM-yy hh.mm.ss a"))
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


      processedEvents.show()

      val dfp = processedEvents.join(df_Meter, "POST_ID")

      val url = "jdbc:postgresql://10.0.0.5:5432/wanlin_db"

      val connectionProperties = new Properties()
      connectionProperties.setProperty("Driver", "org.postgresql.Driver")
      connectionProperties.setProperty("user", "")
      connectionProperties.setProperty("password", "")

      dfp.write.option("header", "true").mode("append").jdbc(url, "Processed_Events", connectionProperties)


      dfp.show()

      println("Job is done")

      spark.stop()
    }
  }

