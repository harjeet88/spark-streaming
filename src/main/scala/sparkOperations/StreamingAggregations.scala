package sparkOperations

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {
  val spark = SparkSession.builder()
    .appName("Agg Example")
    .master("local[2]")
    .getOrCreate()
  def readDf(): DataFrame ={
    val df =spark.readStream
      .format("socket")
      .option("host" , "localhost")
      .option("port" , 12345)
      .load()

    return df
  }

  def writeQuery(df :DataFrame): Unit ={
    df.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
  def getCount(): Unit ={
    val df = readDf()
    val lines = df.selectExpr("count(*) as line_Count")
    writeQuery(lines)
  }

  def groupNames(): Unit ={
    val df = readDf()
    val school= df.select( split(col("value"),",").getItem(0).as("id"),
      split(col("value"),",").getItem(1).as("name"),
      split(col("value"),",").getItem(2).as("age")
    )

    val nameCount = school.groupBy("name").count()

    writeQuery(nameCount)
  }

  def numericAggDemo(func :Column=>Column): Unit ={
    import spark.implicits._
    val df = readDf()

    val numbers = df.select( col("value").cast("Integer").as("number"))
    val aggOut=numbers.select(func(col("number")).as("sum_till_now"))

    writeQuery(aggOut)
  }

  def main(args: Array[String]): Unit = {
    numericAggDemo(mean)
  }
}
