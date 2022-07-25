
import DataQuery.{findMostDeadliestCounty, findMostDeadliestState, spark}

object Main{

  def main(args: Array[String]): Unit = {
    // Initial Test Case
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

    spark.stop()

  }
}