import org.apache.spark

object SSP2Class {

  object Scala {
    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setlevel(Level.ERROR)
      val sc = new SparkContext() //must run on EMR not locally
      val input = sc.textfile("s3n://ssp2emr/time_series_covid_19_deaths_US.csv")
      spark.sql("select _c10 city, cast(_c478 as integer) deaths from US_Deaths order by RAND() limit 5;")
      scala.io.StdIn.readLine()
    }
  }




