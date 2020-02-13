import java.util.Properties
import org.apache.spark.sql.SparkSession
object data_processor_meter_base {
  def main(args: Array[String]) {
    val t0 = System.nanoTime()//start time
    //val csvFile = "s3a://wanlinsinsightmeterdata/sampled_data.csv"
    val csvFile = "s3a://wanlinsinsightmeterdata/Parking_Meters.csv"
    val spark = SparkSession.builder.appName("data_processo").getOrCreate()
    val df = spark.read.format("csv") .option("header", "true").load(csvFile)
    val t1 = System.nanoTime()//data_injest
    println("Data_Injestion: " + (t1 - t0)*1e9d + "s")
    val dfp = df.select("POST_ID", "CAP_COLOR","LONGITUDE","LATITUDE")
    val url = "jdbc:postgresql://10.0.0.5:5432/wanlin_db"

    //Method 1
    val connectionProperties = new Properties()
    connectionProperties.setProperty("Driver", "org.postgresql.Driver")
    connectionProperties.setProperty("user","")
    connectionProperties.setProperty("password","")

    dfp.write.option("header","true").mode("append").jdbc(url, "Parking_Meters", connectionProperties)


    println("Job is done")

    spark.stop()
  }
}


