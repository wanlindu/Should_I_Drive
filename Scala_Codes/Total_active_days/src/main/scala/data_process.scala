import java.util.Properties
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

object data_process{
  def main(args: Array[String]) {
    //val csvFileEvent = "s3a://wanlinsinsightmeterdata/sampled_data.csv"
    val csvFileEvent = "s3a://wanlinsinsightmeterdata/SFMTA_Parking_Meter_Detailed_Revenue_Transactions.csv"
    val spark = SparkSession.builder.appName("data_processo").getOrCreate()
    val df_Event = spark.read.format("csv").option("header", "true").load(csvFileEvent)
    df_Event.show()

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

    processedEvents.show()

    val dfp = processedEvents.groupBy("POST_ID", "Day_Of_Week").count()

    val url = "jdbc:postgresql://10.0.0.5:5432/wanlin_db"

    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user", "wanlin")
    connectionProperties.setProperty("password", "1234")

    dfp.write.option("header", "true").mode("append").jdbc(url, "Total_Active_day_for_each", connectionProperties)

    dfp.show()

    println("Job is done")

    spark.stop()
  }
}

