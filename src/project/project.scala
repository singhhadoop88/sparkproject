package project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source

object project {

  def main(args: Array[String]): Unit = {

    println("----- Step 1 -------")
    println("Creating project")
    println

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val conf = new SparkConf().setMaster("local[*]").setAppName("project").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    println("----- Step 2 -------")
    println("Reading avro file")
    println

    val dfavro = spark
      .read
      .format("avro")
      .load("D:/Mantu's VM/Spark Stuff/project requirement/projectsample.avro")

    dfavro.show()

    println("----- Step 3 -------")
    println("Reading url data")
    println

    val html = Source.fromURL("https://randomuser.me/api/0.8/?results=500")
    val s = html.mkString

    val rdd = sc.parallelize(List(s))

    val dfjson = spark.read.json(rdd)

    dfjson.show()

    println("----- Step 4 -------")
    println("Flatening complex data")
    println

    val dfflat = dfjson.withColumn("results", expr("explode(results)"))

    val dffullflat = dfflat.select(

      "nationality",
      "results.user.cell",
      "results.user.dob",
      "results.user.email",
      "results.user.gender",
      "results.user.location.city",
      "results.user.location.state",
      "results.user.location.street",
      "results.user.location.zip",
      "results.user.md5",
      "results.user.name.first",
      "results.user.name.last",
      "results.user.name.title",
      "results.user.password",
      "results.user.phone",
      "results.user.picture.large",
      "results.user.picture.medium",
      "results.user.picture.thumbnail",
      "results.user.registered",
      "results.user.salt",
      "results.user.sha1",
      "results.user.sha256",
      "results.user.username",
      "seed",
      "version")

    dffullflat.show()

    println("----- Step 5 -------")
    println("Remove numericals from flatten complex data")
    println

    val decimalNumbersPattern = "[0-9]"

    val dfremove = dffullflat.withColumn("username", regexp_replace($"username", decimalNumbersPattern, "").alias("username"))

    dfremove.show()

    println("----- Step 6 -------")
    println("Left join")
    println

    val dfjoin = dfavro.join(dfremove, Seq("username"), "left")

    dfjoin.show()

    println("----- Step 7 -------")
    println("Available and Unavailable customers")
    println

    val dfavail = dfjoin.filter("nationality is not null")

    dfavail.show()

    val dfunavail = dfjoin.filter("nationality is  null")

    dfunavail.show()

  }
  
}