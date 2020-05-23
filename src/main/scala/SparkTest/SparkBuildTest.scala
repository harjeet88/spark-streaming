package SparkTest

import org.apache.spark.sql.SparkSession

object SparkBuildTest {
  val spark = SparkSession.builder()
    .appName("Test App")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    spark.sql("select 'hello' as h, 'hi' as b").show()
  }
}
