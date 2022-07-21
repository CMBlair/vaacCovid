import scala.collection.mutable.Map
/*
def findDeadliestCity(byMonth: Int, andYear: Int): Unit = {
  val daysInMonth = collection.mutable.Map(1 -> 31, 2 -> 28, 3 -> 31,
    4 -> 30, 5 -> 31, 6 -> 30,
    7 -> 31, 8 -> 31, 9 -> 30,
    10 -> 31, 11 -> 30, 12 -> 31)
  if (andYear == 2020) {
    daysInMonth(1) = 9; daysInMonth(2) = 29
  }
  val firstEntryIndex = 12
  val startDayIndex = firstEntryIndex + totalDays
  println("Days in that month:", daysInMonth(byMonth))

}
findDeadliestCity(2, 2020)*/




import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{col, desc, expr, last}

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
    //Setup variables for framing a month by index
    val URBANPOPCRITERIA = 50000
    val header = List("Admin2", "Province_State", "Population", "UID")

    def findDeadliestCity(byMonth: Int, andYear: Int): Unit = {
      val daysInMonth = collection.mutable.Map( 1 -> 31, 2 -> 28, 3 -> 31,
        4 -> 30, 5 -> 31, 6 -> 30,
        7 -> 31, 8 -> 31, 9 -> 30,
        10 -> 31, 11 -> 30, 12 -> 31)
      if (andYear == 2020) {
        daysInMonth(1) = 9; daysInMonth(2) = 29
      }


      val firstEntryIndex = 12
      var totalDays = 0


      for(i <- 1 to byMonth){ totalDays += daysInMonth(i) }


      var startDayIndex = 0 //initializing for later computation
      val endDayIndex = firstEntryIndex + totalDays

      if(byMonth <= 1){
        startDayIndex = firstEntryIndex + totalDays - daysInMonth(byMonth)
      }else{
        startDayIndex = firstEntryIndex + totalDays - daysInMonth(byMonth) + 1
      }


      val testList = header ++ (df.columns.slice(startDayIndex,endDayIndex).toList)

      val testMthDF = df.select(testList.map(m=>col(m)):_*)
      val test = testMthDF.withColumn("Total", ((col(testMthDF.columns(endDayIndex)) - col(testMthDF.columns(startDayIndex)))))

      // Top 5 Most Deadly Cities from Urban Areas
      test.select("*")
        .where(col("Population").>=(URBANPOPCRITERIA))
        .sort(desc("Total")).show(5)

      // Top 5 Most Deadly Cities from Non-Urban Areas
      test.select("*")
        .where(col("Population").<(URBANPOPCRITERIA))
        .where(col("Population").>(0))
        .sort(desc("Total")).show(5)

    }
    findDeadliestCity(6, 2020)

    spark.stop()



