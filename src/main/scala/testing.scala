import DataQuery.{findLeastDeadliestCounty, findMostDeadliestCounty, findMostDeadliestState, spark}


object testing {
  def main(args: Array[String]): Unit = {
    var results: List[String] = List("")
//    for(i <- 1 to 12){
//      results = results ++ List(DataQuery.findDeadliestCity(i, 2020))
//      println(results)
//    }

    val n = 5
    val byMonth = 3
    val andYear = 2021

    // Top n most deadly counties by month
    val result1 = findMostDeadliestCounty(byMonth, andYear, true)
    println(s"The top $n most deadliest counties for the ${byMonth}th Month of Year $andYear")
    result1.show(n)


    // Top n most deadly states by month
    val result2 = findMostDeadliestState(byMonth, andYear, true)
    println(s"The top $n most deadliest states for the ${byMonth}th Month of Year $andYear")
    result2.show(n)

    // Top n least deadly counties by month
    val result3 = findLeastDeadliestCounty(byMonth, andYear, true)
    println(s"The top $n least deadliest counties for the ${byMonth}th Month of Year $andYear")
    result3.show(n)






    spark.stop()



  }
}
