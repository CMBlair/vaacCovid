package Project2
import DataQuery.{findMostDeadliestCounty, findMostDeadliestState, spark}
import org.apache.spark.sql.functions.{col, desc, round}

object Main{

  def main(args: Array[String]): Unit = {
    // Initial Test Case
    val n = 10
    val byMonth = 5
    val andYear = 2021

    // Top n most deadly counties by month for Urban Areas (UAs)
    val result1UA = findMostDeadliestCounty(byMonth, andYear, urbanArea = true)
    val ratioResult1UA = result1UA.withColumn("Death per Population (in %)",  round((col("Total") / col("Population") * 100)
      .cast("float"),2))
      .orderBy(desc("Death per Population (in %)"))

    val result2UA = findMostDeadliestState(byMonth, andYear, urbanArea = true)
    val ratioResult2UA = result2UA.withColumn("Death per Population (in %)",  round((col("Totals by State") / col("Population Total") * 100)
      .cast("float"),2))
      .orderBy(desc("Death per Population (in %)"))

    val result3NUA = findMostDeadliestCounty(byMonth, andYear, urbanArea = false)
    val ratioResult3NUA = result3NUA.withColumn("Death per Population (in %)",  round((col("Total") / col("Population") * 100)
      .cast("float"),2))
      .orderBy(desc("Death per Population (in %)"))

    val result4NUA = findMostDeadliestState(byMonth, andYear, urbanArea = false)
    val ratioResult4NUA = result4NUA.withColumn("Death per Population (in %)",  round((col("Totals by State") / col("Population Total") * 100)
      .cast("float"),2))
      .orderBy(desc("Death per Population (in %)"))


    println(s"The top $n most deadliest counties for Month ${byMonth} of Year $andYear for Urban Areas: ")
    result1UA.show(n)
    println(s"The top $n most deadliest counties for Month ${byMonth} of Year $andYear for Urban Areas by Percentage of Population: ")
    ratioResult1UA.show(n)

    // Top n most deadly states by month for Urban Areas (UAs)

    println(s"The top $n most deadliest states for Month ${byMonth} of Year $andYear for Urban Areas")
    result2UA.show(n)
    println(s"The top $n most deadliest states for Month ${byMonth} of Year $andYear for Urban Areas by Percentage of Population: ")
    ratioResult2UA.show(n)


    // Top n most deadly counties by month for Non-UAs (NUAs)


    println(s"The top $n most deadliest counties for the ${byMonth}th Month of Year $andYear for non-Urban Areas")
    result3NUA.show(n)
    println(s"The top $n most deadliest states for Month ${byMonth} of Year $andYear for non-Urban Areas by Percentage of Population: ")
    ratioResult3NUA.show(n)

    // Top n most deadly states by month for NUAs


    println(s"The top $n most deadliest states for the ${byMonth}th Month of Year $andYear for non-Urban Areas")
    result4NUA.show(n)
    println(s"The top $n most deadliest states for Month ${byMonth} of Year $andYear for non-Urban Areas by Percentage of Population: ")
    ratioResult4NUA.show(n)

    spark.stop()

  }
}