package sparkOperations

import org.apache.spark.sql.SparkSession
import common._
import org.apache.spark.sql.functions._

object StreamingJoinExample {
  val spark = SparkSession.builder()
    .appName("Joins example")
    .master("local[2]")
    .getOrCreate()
  def joinWithStaticDF(): Unit ={
    val empDf=spark.read
      .schema(empSchema)
      .csv("src/main/resources/data/employee/")

    val in = spark.readStream
      .format("socket")
      .option("host" , "localhost")
      .option("port" , 12345)
      .load()
    val depDf = in.select(split(col("value"),",").getItem(0).as("dep_id"),
      split(col("value"),",").getItem(1).as("dep_name")
    )
    val joinedDf = empDf.join(depDf,empDf.col("dep_id")=== depDf.col("dep_id") , "inner")

    joinedDf.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def joinTwoStreamsDemo(): Unit ={
    val empDf = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12345)
      .load()

    val emp = empDf.select(split(col("value"),",").getItem(0).as("id"),
      split(col("value"),",").getItem(1).as("name"),
      split(col("value"),",").getItem(2).as("dep_id")
    )

    val input2= spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",12346)
      .load()

    val dep = input2.select(split(col("value"),",").getItem(0).as("dep_id"),
      split(col("value"),",").getItem(1).as("dep_name")
    )

    emp.join(dep,emp.col("dep_id")===dep.col("dep_id"))
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()




  }

  def main(args: Array[String]): Unit = {
    joinTwoStreamsDemo()
  }
}
