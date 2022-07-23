import DataQuery.spark


object testing {
  def main(args: Array[String]): Unit = {
    var results: List[String] = List("")
    for(i <- 1 to 12){
      results = results ++ List(DataQuery.findDeadliestCity(i, 2020))
      println(results)
    }





    spark.stop()



  }
}
