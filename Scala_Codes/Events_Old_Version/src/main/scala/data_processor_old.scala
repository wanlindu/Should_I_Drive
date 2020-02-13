import java.util.Properties
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
object data_process{
  def main(args: Array[String]) {
    val t0 = System.nanoTime()//start time
    //val csvFile = "s3a://wanlinsinsightmeterdata/sampled_data.csv"
    val csvFile = "s3a://wanlinsinsightmeterdata/SFMTA_Parking_Meter_Detailed_Revenue_Transactions_2ndHalf.csv"
    val spark = SparkSession.builder.appName("data_processo").getOrCreate()

    val customSchema = StructType(Array(StructField("NOT_IN_USE", StringType, true),
      StructField("POST_ID", StringType, true),
      StructField("STREET_BLOCK", StringType, true),
      StructField("PAYMENT_TYPE", StringType, true),
      StructField("SESSION_START_DT", StringType, true),
      StructField("SESSION_END_DT", StringType, true),
      StructField("METER_EVENT_TYPE", StringType, true),
      StructField("GROSS_PAID_AMT", StringType, true)))


    val df = spark.read.format("csv") .option("header", "false").schema(customSchema).load(csvFile)
    val t1 = System.nanoTime()//data_injest
    println("Data_Injestion: " + (t1 - t0)*1e9d + "s")

    val dfp = df.withColumn("start_time", functions.to_timestamp(col("SESSION_START_DT"), "dd-MMM-yy hh.mm.ss a"))
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

    val t2 = System.nanoTime()//data_clean_up
    println("Data_processing: " + (t2 - t1)*1e9d + "s")

    val url = "jdbc:postgresql://10.0.0.14:5432/test"

    //Method 1
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user","")
    connectionProperties.setProperty("password","")

    dfp.write.option("header","true").mode("append").jdbc(url, "cleaned_parking_event_2_6_1", connectionProperties)


    //Method 2
    // val day = List("Mon", "Tue","Wed","Thu","Fri","Sat","Sun")
    // for(a <- day){
    //    var dfp_s = dfp.filter(col("Day_Of_Week")=== a)
    //    dfp_s.write.option("header","true").mode("append").jdbc(url, "test_2_4_1"+a, connectionProperties)
    // }


    println("Job is done")

    spark.stop()
  }
}

