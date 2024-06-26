package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  //create a spark session
  val spark = SparkSession.builder()
    .appName("DataFrameBasics")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._


  val carsPath = "src/main/resources/data/cars.json"
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(carsPath)

  //show DF
  firstDF.show()
  firstDF.printSchema()

  //get rows
  firstDF.take(10).foreach(println)

  //spark types use singletons
  val longType = LongType

  //schemas
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // get schema of df
  val carsDFschema = firstDF.schema
  // println(carsDFschema)

  // use schema to read df
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load(carsPath)


  //create individual rows by hand
  // schema-less data structure
  val myRow = Row("chevrolet chevelle malibu",18.0,8,307.0,130,3504,12.0,"1970-01-01","USA")

  //create multiple rows by hand with Seq
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) //schema auto-inferred here

  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")


  /*
  * Exercises
  * 1) build similar data structure for smartphones
  *
  * 2) read movies.json and get count
  * */

  val manualSmartPhonesSchema = StructType(Array(
    StructField("phone_name", StringType),
    StructField("phone_height_inches", DoubleType)
  ))

  val moviesDF = spark.read
    .format("json")
    .load("src/main/resources/data/movies.json")

  val moviesDFCount = moviesDF.count()
  println(s"the movies df has a count of: ${moviesDFCount} rows ")
}
