package rj.spark.data.pipeline.hr.context

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SqlContext {

  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")
    .set("spark.local.dir", "/tmp/spark-temp")
    .set("spark.sql.catalogImplementation", "hive")
    .set("spark.sql.warehouse.dir", "./spark-warehouse")

  lazy val spark = SparkSession
    .builder()
    .enableHiveSupport
    .config(sparkConf)
    .getOrCreate()

  lazy val sqlContext = spark.sqlContext

}
