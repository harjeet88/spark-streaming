package streamingAdvanced

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object StreamingWindowsExample {
  val spark = SparkSession.builder()
    .appName("Windows Example")
    .master("local[2]")
    .getOrCreate()

  val salesSchema = StructType(Array(
    StructField("id",IntegerType),
    StructField("time",TimestampType),
    StructField("item",StringType),
    StructField("quantity",IntegerType)
  ))

  def readSocket(): DataFrame  ={
    val df = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()
      .select(from_json(col("value"),salesSchema).as("sales"))
      .selectExpr("sales.*")
    return df
  }

  def tumblingWindowDemo(): Unit ={
    val df =readSocket()

    val windowCol=window(col("time"),"5 minutes").as("timeCol")

    val counts = df.groupBy(windowCol)
      .agg(sum(col("quantity")).as("totalQuantity"))
      .select(col("timeCol").getField("start").as("start"),
        col("timeCol").getField("end").as("end"),
        col("totalQuantity")
      )
    writeQuery(counts)
  }

  def slidingWindowDemo(): Unit ={
    val df = readSocket()

    val windowCol=window(col("time"),"5 minutes" ,"1 minute").as("timeCol")
    val counts = df.groupBy(windowCol)
      .agg(sum(col("quantity")).as("totalQuantity"))
      .select(col("timeCol").getField("start").as("start"),
        col("timeCol").getField("end").as("end"),
        col("totalQuantity")
      )
    writeQuery(counts)
  }


  def writeQuery(df:DataFrame): Unit ={
    df.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }


  def main(args: Array[String]): Unit = {
      slidingWindowDemo()
  }

}
