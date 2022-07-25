import DataQuery.{findLeastDeadliestCounty, findMostDeadliestCounty, findMostDeadliestState, spark}
import org.apache.spark.sql.functions.{col, desc, round}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, functions}

import scala.collection.mutable.ArrayBuffer


object testing {
  def main(args: Array[String]): Unit = {
    val n = 5
    val byMonth = 8
    val andYear = 2020
    // Top n most deadly counties by month for Urban Areas (UAs)
//    val result1a = findMostDeadliestCounty(byMonth, andYear, true)
//    val ratioResult1a = result1a.withColumn("Death per Population",  round((col("Total") / col("Population") * 100)
//      .cast("float"),2))
//      .orderBy(desc("Death per Population"))
//    result1a.show(10)
//    ratioResult1a.show(10)

    val results: ArrayBuffer[DataFrame] = ArrayBuffer()

    for(i <- 1 to 12){
      results.append(DataQuery.findMostDeadliestState(i, 2020, true))
      if(1 < i){
        results(i-2) = results(i-2)
          .withColumnRenamed("Totals by State", s"Totals Month ${(i-1)}")
          .drop("Population Total")
          .join(results(i-1), "State")
          .withColumnRenamed("Totals by State", s"Totals Month ${(i)}")
          .withColumn("Rate of Change", ((col(s"Totals month ${(i)}") - col(s"Totals month ${(i-1)}"))/col(s"Totals month ${(i-1)}")))
          .drop("Population Total")
          .sort(desc("Rate of Change"), (desc(s"Totals Month ${i}")))

        results(i-2).show(5)
      }
    }

//    var newDF:DataFrame = for (i <- 1 to 11) yield val temp = results(i - 1).join(results(i), ("State"))






    // Top n least deadly counties by month
//    val result3 = findLeastDeadliestCounty(byMonth, andYear, true)
//    println(s"The top $n least deadliest counties for the ${byMonth}th Month of Year $andYear")
//    result3.show(n)






    spark.stop()



  }
}
