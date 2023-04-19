package project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



object project2 {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val conf = new SparkConf().setMaster("local[*]").setAppName("project").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val df1 = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("D:/Mantu's VM/Spark Stuff/project2/table1.csv")

    df1.show()

    val df2 = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("D:/Mantu's VM/Spark Stuff/project2/table2.csv")

    df2.show()

    val dftype = df1.select(col("order_no"), split(col("attribute_names"), ",").alias("attribute_names"))

    dftype.printSchema()

    dftype.show()

    // val dfexpl = dftype.select(explode(col("attribute_names")).as("attribute_names"),col("order_no"))

    val dfexpl = dftype.withColumn("attribute_names", expr("explode(attribute_names)"))

    dfexpl.printSchema()
    dfexpl.show()

    dfexpl.createOrReplaceTempView("order_dtl")

    val dfrank = spark.sql("select order_no,attribute_names from  (select order_no,attribute_names,rank() over (partition by order_no order by attribute_names) as rank from order_dtl) where rank=1 order by order_no")

    // val dffilt = dfrank.filter("rank = 1")

    dfrank.show()

    val dfjoin = dfrank.join(df2, dfrank("attribute_names") === df2("attribute_name"), "inner").select("order_no", "theme")

    dfjoin.show()
  }          
    
  }