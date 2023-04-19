package project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column



object uber {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val conf = new SparkConf().setMaster("local[*]").setAppName("uber").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val df = spark
      .read
      .format("csv")
      .option("header", "true")
      .load("D:/Mantu's VM/Spark Stuff/project3_uberproject/uber.csv")

    df.show()

    val dfdate = df.withColumn("date", unix_timestamp(col("date"), "MM/dd/yyyy").cast("timestamp"))

    dfdate.show()

    df.printSchema()

    val dfmon = dfdate.withColumn("dow", date_format(col("date"), "EEE"))

    dfmon.show()

    val dfmoncnt = dfmon.withColumn("downum", expr("case when dow = 'Sun' then 0 when dow='Mon' then 1 when dow='Tue' then 2 when dow='Wed' then 3 when dow='Thu' then 4 when dow='Fri' then 5 when dow='Sat' then 6 else null end").cast("Int"))

    dfmoncnt.show()

    val dfpart = dfmoncnt.select("*").orderBy("dispatching_base_number", "downum", "date")

    dfpart.show(100)

    val dftrip = dfpart.withColumn("trips", expr("trips").cast("Int"))

    dftrip.printSchema()

    dftrip.createOrReplaceTempView("maxtab")

    val dffinal = spark.sql("select * from maxtab where (dispatching_base_number,downum,trips) in (select distinct dispatching_base_number,downum , max(trips) over (partition by dispatching_base_number,downum ) from maxtab ) order by dispatching_base_number,downum")

    dffinal.show(100)
    
    dffinal.coalesce(1).write.format("csv").save("D:/Mantu's VM/Spark Stuff/project3_uberproject/uber_final.csv")
    
    println("data written")

  }
  
  
}