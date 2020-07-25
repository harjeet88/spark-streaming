package streamingAdvanced

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object WatermarkExample {
  val spark = SparkSession.builder()
    .appName("watermark ex")
    .master("local[*]")
    .getOrCreate()

  val salesSchema = StructType(Array(
    StructField("id",IntegerType),
    StructField("time",TimestampType),
    StructField("item",StringType),
    StructField("quantity",IntegerType)
  ))

  def readSocket():DataFrame={
    val df = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()
      .select(from_json(col("value"),salesSchema).as("sales"))
      .selectExpr("sales.*")
    return df
  }

  def watermarkDemo(): Unit ={
    val df = readSocket()
    val windowCol = window(col("time"),"6 minutes","2 minutes").as("timeCol")
    val watermarkDf = df.withWatermark("time","10 minutes")
      .groupBy(windowCol)
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("timeCol").getField("start").as("start"),
        col("timeCol").getField("end").as("end"),
        col("totalQuantity")
      )
    writeQueryAndStart(watermarkDf)

  }

  def writeQueryAndStart(df:DataFrame)={
    val qry = df.writeStream
      .format("console")
      .outputMode("append")
      .start()

    debugQuery(qry)
    qry.awaitTermination()
  }

  def debugQuery(query: StreamingQuery)={
      new Thread(() =>{
        (1 to 10000).foreach{ i =>
          Thread.sleep(500)
          if(query.lastProgress == null){
            print("[]")
          }else{
            val watermark = query.lastProgress.eventTime.get("watermark").toString
            val wmTime=Timestamp.from(Instant.parse(watermark))
            println(wmTime)
          }
        }
      }).start()

  }

  def main(args: Array[String]): Unit = {
    watermarkDemo()
  }

}
