package part2dataframes
// NOTE: I skipped some of the code on data types that I see at work a lot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

object DataSources extends App {
  //create a spark session
  val spark = SparkSession.builder()
    .appName("DataFrameBasics")
    .config("spark.master", "local")
    .getOrCreate()

  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))
 // so many options... reasons to dislike csv
  val stocksDF = spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateformat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // reading from a remote database
  // there has to be a better data struc for this
  val pgDriver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker" // this is typically handled with a secret manager
  val password =  "docker" // ditto


  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker") // this is typically handled with a secret manager
    .option("password", "docker") // ditto
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()


  /*
  * Excercise:
  *   1) write movies as tab seperated
  *   2) write as parquet
  *   3) write in postgresql as "public.movies"
  *
  * */

  //1
  val moviesDF = spark.read.format("json").load("src/main/resources/data/movies.json")
  // write data
  moviesDF.write.format("csv").option("header","true").option("sep", "\t")
    .save("src/main/resources/data/movies.csv")
  // parquet (snappy is default compression type)
  moviesDF.write.save("src/main/resources/data/movies.parquet")
  // database
  moviesDF.write.format("jdbc")
    .option("driver", pgDriver)
    .option("user", user)
    .option("password", password)
    .option("url", url)
    .option("dbtable","public.movies")
    .save()

}
