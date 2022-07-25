import DataQuery.{findLeastDeadliestCounty, findMostDeadliestCounty, findMostDeadliestState, spark}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER

import scala.collection.mutable.ArrayBuffer


object testing {
  def main(args: Array[String]): Unit = {
    val results: ArrayBuffer[DataFrame] = ArrayBuffer()
    for(i <- 1 to 12){
      results.append(DataQuery.findMostDeadliestState(i, 2020, true))
      results(i-1).persist(MEMORY_AND_DISK_SER)
    }
//    var newDF:DataFrame = for (i <- 1 to 11) yield val temp = results(i - 1).join(results(i), ("State"))

    val n = 5
    val byMonth = 8
    val andYear = 2020

    // Top n most deadly counties by month for Urban Areas (UAs)
    val result1a = findMostDeadliestCounty(byMonth, andYear, true)
    println(s"The top $n most deadliest counties for the ${byMonth}th Month of Year $andYear for Urban Areas")
    result1a.show(n)

    // Top n most deadly states by month for Urban Areas (UAs)
    val result2a = findMostDeadliestState(byMonth, andYear, true)
    println(s"The top $n most deadliest states for the ${byMonth}th Month of Year $andYear for Urban Areas")
    result2a.show(n)
    // Top n most deadly counties by month for Non-UAs (NUAs)
    val result1b = findMostDeadliestCounty(byMonth, andYear, false)
    println(s"The top $n most deadliest counties for the ${byMonth}th Month of Year $andYear for non-Urban Areas")
    result1b.show(n)
    // Top n most deadly states by month for NUAs
    val result2b = findMostDeadliestState(byMonth, andYear, false)
    println(s"The top $n most deadliest states for the ${byMonth}th Month of Year $andYear for non-Urban Areas")
    result2b.show(n)



    // Top n least deadly counties by month
//    val result3 = findLeastDeadliestCounty(byMonth, andYear, true)
//    println(s"The top $n least deadliest counties for the ${byMonth}th Month of Year $andYear")
//    result3.show(n)






    spark.stop()



  }
}
