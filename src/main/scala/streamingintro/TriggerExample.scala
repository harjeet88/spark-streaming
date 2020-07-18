package streamingintro

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object TriggerExample {
  val spark = SparkSession.builder()
    .appName("Trigger Demo")
    .master("local[2]")
    .getOrCreate()

  def triggerDemo(): Unit ={
    val df = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port", 12345)
      .load()

    df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpoint", "src/main/chk/")
      .trigger(Trigger.Continuous(2.seconds))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    triggerDemo()
  }

}
