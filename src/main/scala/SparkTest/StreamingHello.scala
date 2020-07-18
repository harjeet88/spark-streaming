package SparkTest

import org.apache.spark.sql.SparkSession

object StreamingHello {
  val spark = SparkSession.builder()
    .appName("Hello App")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket(){
    val lines = spark.readStream.format("socket")
      .option("host","localhost")
      .option("port","22222")
      .load()

    val linesQuery = lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    linesQuery.awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    readFromSocket()
  }
}
