package streamingintro

import org.apache.spark.sql.SparkSession
import common._

object FolderStreamingExample {
  val spark = SparkSession.builder()
    .appName("Folder Streaming Example Demo")
    .master("local[2]")
    .getOrCreate()

  def readFolderData() ={
    val df = spark.readStream
      .format("csv")
      .option("header" , false)
      .schema(studentSchema)
      .load("src/main/resources/data/stream_input/")

    df.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()


  }

  def main(args: Array[String]): Unit = {
    readFolderData()
  }
}
