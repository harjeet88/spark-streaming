package streamingintro

import org.apache.spark.sql.SparkSession

object SparkStreamingHello {
  val spark = SparkSession.builder()
    .appName("Spark Streaming Hello")
    .master("local[2]")
    .getOrCreate()
  def getSocketData(): Unit ={
    val lines = spark.readStream.format("socket")
      .option("host","localhost")
      .option("port", "22222")
      .load()
    val queryLines = lines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    queryLines.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    getSocketData()
  }

}
