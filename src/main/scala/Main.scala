
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, desc, expr, last}

object Main{
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

    // Please remember to update hdfs location path
    val df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/akiem/time_series_covid_19_deaths_US.csv")


    //For Jul 1, 2020 - Jul 31, 2020
    //Setup variables
    val URBANPOPCRITERIA = 50000
    val startDay = 173
    val endDay = 204


    //**************Return to renaming header columns***********

    val header = List("Admin2", "Province_State", "Population", "UID")
    val testList = header ++ (df.columns.slice(startDay,endDay).toList)




    val testMthDF = df.select(testList.map(m=>col(m)):_*)
    val test = testMthDF.withColumn("Total", ((col(testMthDF.columns(34)) - col(testMthDF.columns(4)))))

    //header.show()
    //testMthDF.show()
    //test.show()

    // Top 5 Most Deadly Cities from Urban Areas
    test.select("*")
      .where(col("Population").>=(URBANPOPCRITERIA))
      .sort(desc("Total")).show(5)

    // Top 5 Most Deadly Cities from Non-Urban Areas
    test.select("*")
      .where(col("Population").<(URBANPOPCRITERIA))
      .where(col("Population").>(0))
      .sort(desc("Total")).show(5)


    spark.stop()

  }
}