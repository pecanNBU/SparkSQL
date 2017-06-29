/**
  * Created by Administrator on 2017-6-28.
  */

import org.apache.spark
import org.apache.spark.sql.{Row, SparkSession}

object SparkSQL {

  case class Person(name: String, age: Long)
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("spark sql test")
    .config("spark.some.config.option", "some value")
    .getOrCreate()
  def main(args: Array[String]) {
    /*val spark = SparkSession.builder()
      .master("local")
      .appName("spark sql test")
      .config("spark.some.config.option", "some value")
      .getOrCreate()
    val df = spark.read.textFile("file:///D:/people.txt")
    df.show()
    df.printSchema()
    //For implicit conversions from RDDs to DataFrames
    import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.read.textFile("file:///D:/people.txt").map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")
    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = spark.sql("select name,age from people")
    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "name:" + teenager(0)).show()
    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    // Array(Map("name" -> "Justin", "age" -> 19))*/

    SpecifyingSchema()
    CreateDatasets()
  }

  def SpecifyingSchema(): Unit = {
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.Encoder
    import sparkSession.implicits._
    // Create an RDD
    val peopleRDD = sparkSession.sparkContext.textFile("file:///d:/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = sparkSession.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = sparkSession.sql("SELECT name,age FROM people").toDF().show()
    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    //results.map(attributes => "Name: " + attributes(0)+" "+"age:"+attributes(1)).show()
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
  }
  def CreateDatasets(): Unit ={
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("spark sql test")
      .config("spark.some.config.option", "some value")
      .getOrCreate()
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.Encoder
    import sparkSession.implicits._
    // Create an RDD
    val peopleRDD = sparkSession.sparkContext.textFile("file:///D:/people.txt").toDS().show()

    val peopleDF = sparkSession.sparkContext.textFile("file:///D:/people.txt").map(_.split(",")).map(attributes => Person(attributes(0), attributes(1).trim.toInt)).toDS().show()
    //val caseClassDS=peopleRDD.toDS().show()
    //spark.sparkContext与spark.read 区别
  }

}
