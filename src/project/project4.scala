package project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column



object project4 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val conf = new SparkConf().setMaster("local[*]").setAppName("date_project").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    
    
    val df = spark
              .read
              .format("csv")
              .option("header", "true")
              .load("D:/Mantu's VM/Spark Stuff/projet4/moh.csv")
              
              df.show()
              
              df.printSchema()
              
              
              
      val dfadddays = df.withColumn("New_Date", expr("date_add(date,remaining_days)")).show()
    
  }
  
}