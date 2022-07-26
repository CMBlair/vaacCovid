package Project2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{asc, col, desc, round, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DataQuery {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark: SparkSession = SparkSession
    .builder
    .appName("Data Query App")
    .master("local")
    //.enableHiveSupport()
    .config("spark.master", "local")
    .config("spark.eventLog.enabled", value = false)
    .getOrCreate()


  //For Jul 1, 2020 - Jul 31, 2020
  //Setup variables for framing a month by index
  val URBANPOPCRITERIA: Int = 50000
  val header: List[String] = List("Admin2", "Province_State", "Population", "UID")



  def findDeadliestCity(byMonth: Int = 7, andYear: Int = 2020, urbanArea: Boolean = true): DataFrame = {
    // Please remember to update hdfs location path
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val df = spark.read.format("csv").option("header", "true").load("hdfs://localhost:9000/user/akiem/time_series_covid_19_deaths_US.csv")
    val daysInMonth = collection.mutable.Map(1 -> 31, 2 -> 28, 3 -> 31,
      4 -> 30, 5 -> 31, 6 -> 30,
      7 -> 31, 8 -> 31, 9 -> 30,
      10 -> 31, 11 -> 30, 12 -> 31)
    if (andYear == 2020) {
      daysInMonth(1) = 9
      daysInMonth(2) = 29
    }


    val firstEntryIndex = 12
    var totalDays = 0


    for (i <- 1 to byMonth) {
      totalDays += daysInMonth(i)
    }


    var startDayIndex = 0 //initializing for later computation
    val endDayIndex = firstEntryIndex + totalDays + 1

    if (byMonth <= 1) {
      startDayIndex = firstEntryIndex + totalDays - daysInMonth(byMonth)
    } else {
      startDayIndex = firstEntryIndex + totalDays - daysInMonth(byMonth) + 1
    }


    val testList: List[String] = header:::df.columns.slice(startDayIndex, endDayIndex).toList
    val startDay = 4 // per new testListDF columns with added header columns
    val endDay = daysInMonth(byMonth) + startDay - 1

    val testMthDF = df.select(testList.map(m => col(m)): _*).withColumnRenamed("Admin2", "County")
      .withColumnRenamed("Province_State", "State")
    val test = testMthDF.withColumn("Total", col(testMthDF.columns(endDay)) - col(testMthDF.columns(startDay)))


    // Top 5 Most Deadly Cities from Urban Areas
    if (urbanArea) {
      test.select("*")
        .where(col("Population").>=(URBANPOPCRITERIA))
        .sort(desc("Total"))
    } else {
      // Top 5 Most Deadly Cities from Non-Urban Areas
      test.select("*")
        .where(col("Population").<(URBANPOPCRITERIA))
        .where(col("Population").>(0))
        .sort(desc("Total"))
    }

    //    return test.select("Admin2")
    //               .where(col("Population").>=(URBANPOPCRITERIA)).sort(desc("Total"))


  }

  // Function to find the top n most deadly counties based on month and year
  def findMostDeadliestCounty(byMonth: Int, andYear: Int, urbanArea: Boolean): DataFrame = {
    // given the month (byMonth) the year (andYear) and preference to urban or non-urban areas find the most Deadliest County
    // call to findDeadliestCity to parse out the monthly data
    val header: List[String] = List("County", "State", "Population", "UID")
    val testList: List[String] = header ++ List("Total")
    if (urbanArea) {
      val dfTest = findDeadliestCity(byMonth, andYear).select(testList.map(m => col(m)): _*).sort(desc("Total"))
      dfTest
    } else {
      val dfTest = findDeadliestCity(byMonth, andYear, urbanArea).select(testList.map(m => col(m)): _*).sort(desc("Total"))
      dfTest
    }
  }

  // Function to find the top n most deadly states based on month and year
  def findMostDeadliestState(byMonth: Int, andYear: Int, urbanArea: Boolean): DataFrame = {
    // TODO: Input what function does!
    val header = List("County", "State", "Population", "UID")
    val testList = header ++ List("Total")
    if (urbanArea) {
      val dfTest = findDeadliestCity(byMonth, andYear, urbanArea)
        .select(testList.map(m => col(m)): _*)
        .groupBy("State")
        .agg(sum("Total").as("Totals by State"), sum("Population").cast("Long").as("Population Total"))
        .sort(desc("Totals by State"))
      dfTest
    } else {
      val dfTest = findDeadliestCity(byMonth, andYear, urbanArea)
        .select(testList.map(m => col(m)): _*)
        .groupBy("State")
        .agg(sum("Total").as("Totals by State"), sum("Population").cast("Long").as("Population Total"))
        .sort(desc("Totals by State"))
      dfTest
    }

  }

  // Function to find the top n least deadly counties based on month and year
  def findLeastDeadliestCounty(byMonth: Int, andYear: Int, urbanArea: Boolean): DataFrame = {
    // given the month (byMonth) the year (andYear) and preference to urban or non-urban areas find the most Deadliest County
    // call to findDeadliestCity to parse out the monthly data
    val header = List("County", "State", "Population", "UID")
    val testList = header ++ List("Total")
    val dfTest = findDeadliestCity(byMonth, andYear).select(testList.map(m => col(m)): _*).where("Total > 0").sort(asc("Total"))
    dfTest
  }

  // TODO: Function to find the top n most deadly month and year
  //  def findDeadliestMonth(byState: String, byYear: Int, urbanArea: Boolean):DataFrame = {
  //    for(i <- 1 to 12-1)
  //      for(j <- 2 to 12){
  //        val dfResult = findMostDeadliestState(i, byYear, true).where(col("State").===(byState)).take(1).toArray
  //        println(dfResult(0))
  //    }
  //  }


  def saveToParquet(): Unit = {
    // Save UAs via parquet
    val results: ArrayBuffer[DataFrame] = ArrayBuffer()
    val results1UA: ArrayBuffer[DataFrame] = ArrayBuffer()
    val ratioResults1UA: ArrayBuffer[DataFrame] = ArrayBuffer()
    val results2UA: ArrayBuffer[DataFrame] = ArrayBuffer()
    val ratioResults2UA: ArrayBuffer[DataFrame] = ArrayBuffer()
    val results3NUA: ArrayBuffer[DataFrame] = ArrayBuffer()
    val ratioResults3NUA: ArrayBuffer[DataFrame] = ArrayBuffer()
    val results4NUA: ArrayBuffer[DataFrame] = ArrayBuffer()
    val ratioResults4NUA: ArrayBuffer[DataFrame] = ArrayBuffer()


    val andYear = 2020

    for (byMonth <- 1 to 4) {
      results.append(DataQuery.findMostDeadliestState(byMonth, andYear, true))
      if (1 < byMonth) {
        results(byMonth - 2) = results(byMonth - 2)
          .withColumnRenamed("Totals by State", s"Totals_Month_${(byMonth - 1)}_Year_${andYear}")
          .drop("Population Total")
          .join(results(byMonth - 1), "State")
          .withColumnRenamed("Totals by State", s"Totals_Month_${(byMonth)}_Year_${andYear}")
          .withColumn("Rate_of_Change", ((col(s"Totals_Month_${(byMonth)}_Year_${andYear}") - col(s"Totals_Month_${(byMonth - 1)}_Year_${andYear}")) / col(s"Totals_Month_${(byMonth - 1)}_Year_${andYear}")))
          .drop("Population Total")
          .sort(desc("Rate_of_Change"), (desc(s"Totals_Month_${byMonth}_Year_${andYear}")))

        results1UA(byMonth - 1) = findMostDeadliestCounty(byMonth, andYear, urbanArea = true)
        ratioResults1UA(byMonth - 1) = results1UA(byMonth - 1).withColumn("Death per Population (in %)",  round((col("Total") / col("Population") * 100)
          .cast("float"),2))
          .orderBy(desc("Death per Population (in %)"))

        results2UA(byMonth - 1) = findMostDeadliestState(byMonth, andYear, urbanArea = true)
        ratioResults2UA(byMonth - 1) = results2UA(byMonth - 1).withColumn("Death per Population (in %)",  round((col("Totals by State") / col("Population Total") * 100)
          .cast("float"),2))
          .orderBy(desc("Death per Population (in %)"))

        results3NUA(byMonth - 1) = findMostDeadliestCounty(byMonth, andYear, urbanArea = false)
        ratioResults3NUA(byMonth - 1) = results3NUA(byMonth - 1).withColumn("Death per Population (in %)",  round((col("Total") / col("Population") * 100)
          .cast("float"),2))
          .orderBy(desc("Death per Population (in %)"))

        results4NUA(byMonth - 1) = findMostDeadliestState(byMonth, andYear, urbanArea = false)
        ratioResults4NUA(byMonth - 1) = results4NUA(byMonth - 1).withColumn("Death per Population (in %)",  round((col("Totals by State") / col("Population Total") * 100)
          .cast("float"),2))
          .orderBy(desc("Death per Population (in %)"))

        results(byMonth - 2).show(5)
        results(byMonth - 2).write.format("parquet")
          .mode("append")
          .save(f"/user/hive/warehouse/testing1/UA/${andYear}/Top_Ten_Deadly_States_${byMonth}")


        //Saving most deadliest counties
        results(byMonth - 1).show(5)
        results(byMonth - 1).write.format("parquet")
          .mode("append")
          .save(f"/user/hive/warehouse/testing1/UA/${andYear}/Top_Ten_Most_Deadly_Counties_${byMonth}")

      }
    }
  }

  // Function to find the top n least deadly month and year


  // What month had the highest rate of change for Covid deaths?

  // What was the average number of confirmed cases for urban versus non-urban areas?

  // Do changes in non-urban area data show similar trends in urban areas as well?


}
