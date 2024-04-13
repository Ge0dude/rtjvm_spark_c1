package part2dataframes

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder().appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //carsDF.show()

  val firstColumn = carsDF.select("Name")

  //lots of ways to select columns

  // passing in column objects and expressions
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"), //method on dataframes .col()
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, //Scala Symbol,
    $"Horsepower", //interpolated string
    expr("Origin") // expression
  )

  //using strings to select columns
  carsDF.select("Name", "Acceleration")

  //selecting is a narrow transformation
  // all input partitions have exactly one corresponding output partition

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression: Column = {
    carsDF.col("Weight_in_lbs") / 2.2
  }

  val carsWithWeightsDf: DataFrame = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kgs"),
    expr("Weight_in_lbs / 2.2")
  )
  // expr division is implemented differently than Scala division... things to watch out for
  carsWithWeightsDf.show()

  // DF processing
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kgs_3", col("Weight_in_lbs") / 2.2)

  // drop columns
  // more nature than using * operator on a iterator in python
  val carsWithoutCylinders = carsDF.drop("Cylinders", "Displacement")

  // filtering
  carsDF.filter(col("Origin") =!= "USA") // same as .where()
  carsDF.filter(col("Origin") === "USA")
  carsDF.where("Origin = 'USA' ")

  // filters can be chained or chained with boolean logic
  carsDF.where(col("Origin") === "USA" and col("Horsepower") > 150)
  // or use sql like strings
  carsDF.where("Origin = 'USA' and Horsepower > 160")

  //UNION to add rows
  



}
