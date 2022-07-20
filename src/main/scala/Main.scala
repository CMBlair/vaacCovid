
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, expr, last}

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .appName("Hello Spark App")
      //.master("local")
      //.enableHiveSupport()
      .config("spark.master", "local")
      .config("spark.eventLog.enabled", false)
      .getOrCreate()
    val df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/akiem/time_series_covid_19_deaths_US.csv")
    val header = df.select("Admin2", "Province_State", "Population")

    //For Jul 1, 2020 - Jul 31, 2020
    val startDay = 172
    val endDay = 204
    val testMthDF = df.select((df.columns.slice(startDay, endDay).map(m=>col(m))):_*)
    val test = testMthDF.withColumn("Total", ((col(testMthDF.columns(31)) - col(testMthDF.columns(1)))))
    //header.join(test, "*").show()
    header.show()
    testMthDF.show()
    test.show()



    spark.stop()

  }
}