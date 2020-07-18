import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

package object common {

  val studentSchema= StructType(Array(
    StructField("id",IntegerType),
    StructField("name",StringType),
    StructField("age",IntegerType)
  ))

  val empSchema=StructType(Array(
    StructField("id",IntegerType),
    StructField("name",StringType),
    StructField("dep_id",IntegerType)
  )
  )

}
