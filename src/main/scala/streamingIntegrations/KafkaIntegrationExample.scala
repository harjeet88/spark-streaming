package streamingIntegrations

import org.apache.spark.sql.SparkSession
import common._

object KafkaIntegrationExample {
  val spark = SparkSession.builder()
    .appName("Kafka Integration")
    .master("local[2]")
    .getOrCreate()
  def kafkaDemo(): Unit ={
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers" , "localhost:9092")
      .option("subscribe" , "dataSavvy")
      .load()

    df.selectExpr("cast(value as string) as msg")
      .writeStream
      .format("console")
      .outputMode("append")
      .option("checkpoint","src/main/kafka/")
      .start()
      .awaitTermination()
  }

  def writeToKafka(): Unit ={
    val df = spark.readStream
      .schema(studentSchema)
      .csv("src/main/resources/data/stream_input/")

    df.selectExpr("cast(id as string) as key","name as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers" , "localhost:9092")
      .option("topic" , "dataSavvy")
      .option("checkpointLocation","src/main/kfka")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka()
  }
}
