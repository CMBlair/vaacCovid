
import DataQuery.{findMostDeadliestCounty, findMostDeadliestState, spark}
import org.apache.spark.sql.functions.{col, desc, round}

object Main{

  def main(args: Array[String]): Unit = {
    // Initial Test Case
    val n = 10
    val byMonth = 5
    val andYear = 2021

    // Top n most deadly counties by month for Urban Areas (UAs)
    val result1a = findMostDeadliestCounty(byMonth, andYear, true)
    val ratioResult1a = result1a.withColumn("Death per Population",  round((col("Total") / col("Population") * 100)
      .cast("float"),2))
      .orderBy(desc("Death per Population"))

    println(s"The top $n most deadliest counties for the ${byMonth}th Month of Year $andYear for Urban Areas: ")
    result1a.show(n)
    println(s"The top $n most deadliest counties for the ${byMonth}th Month of Year $andYear for Urban Areas by Percentage of Population: ")
    ratioResult1a.show(n)
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