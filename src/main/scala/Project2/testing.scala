package Project2

import Project2.DataQuery.spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc}

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

    for (i <- 1 to 12) {
      results.append(DataQuery.findMostDeadliestState(i, andYear, true))
      if (1 < i) {
        results(i - 2) = results(i - 2)
          .withColumnRenamed("Totals by State", s"Totals_Month_${(i - 1)}_Year_${andYear}")
          .drop("Population Total")
          .join(results(i - 1), "State")
          .withColumnRenamed("Totals by State", s"Totals_Month_${(i)}_Year_${andYear}")
          .withColumn("Rate_of_Change", ((col(s"Totals_Month_${(i)}_Year_${andYear}") - col(s"Totals_Month_${(i - 1)}_Year_${andYear}")) / col(s"Totals_Month_${(i - 1)}_Year_${andYear}")))
          .drop("Population Total")
          .sort(desc("Rate_of_Change"), (desc(s"Totals_Month_${i}_Year_${andYear}")))

        results(i - 2).show(5)
        /*results(i - 2).write.format("parquet")
                      .mode("append")
                      //.saveAsTable("Top_Ten_Deadly_States")
                      .save(f"/user/hive/warehouse/testing1/UA/${andYear}/Top_Ten_Deadly_States_${i}")*/
      }
    }

//    for (i <- 1 to 4) {
//      results.append(DataQuery.findMostDeadliestState(i, (andYear+1), true))
//      if (1 < i) {
//        results(i - 2) = results(i - 2)
//          .withColumnRenamed("Totals by State", s"Totals_Month_${(i - 1)}_Year_${andYear+1}")
//          .drop("Population Total")
//          .join(results(i - 1), "State")
//          .withColumnRenamed("Totals by State", s"Totals_Month_${(i)}_Year_${andYear+1}")
//          .withColumn("Rate_of_Change", ((col(s"Totals_Month_${(i)}_Year_${andYear+1}") - col(s"Totals_Month_${(i - 1)}_Year_${andYear+1}")) / col(s"Totals_Month_${(i - 1)}_Year_${andYear+1}")))
//          .drop("Population Total")
//          .sort(desc("Rate_of_Change"), (desc(s"Totals_Month_${i}_Year_${andYear+1}")))
//
//        results(i - 2).show(5)
//        results(i - 2).write.format("parquet")
//          .mode("append")
//          .saveAsTable(f"/user/hive/warehouse/testing/UA/${andYear+1}/Top_Ten_Deadly_States_${i}")
//      }
//    }


    //    var newDF:DataFrame = for (i <- 1 to 11) yield val temp = results(i - 1).join(results(i), ("State"))


    // Top n least deadly counties by month
    //    val result3 = findLeastDeadliestCounty(byMonth, andYear, true)
    //    println(s"The top $n least deadliest counties for the ${byMonth}th Month of Year $andYear")
    //    result3.show(n)






    spark.stop()


  }
}
