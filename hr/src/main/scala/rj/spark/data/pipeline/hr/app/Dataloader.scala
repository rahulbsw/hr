package rj.spark.data.pipeline.hr.app

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import rj.spark.data.pipeline.hr.context.Context
import rj.spark.data.pipeline.hr.util.Transfromation

/** Load CSV file
  *
  * @author rjain
  */
object Dataloader extends App with Context {

  import rj.spark.data.pipeline.hr.util.Transfromation._
  import rj.spark.data.pipeline.hr.util.Utility._
  import spark.implicits._

  //read 2 lines of the file
  val data = spark.read
    .textFile("data/input/csvFile")
    .limit(2)
  //Read header
  val header = data.first()
  val testRecord = data.take(1)(0)

  // trim header write into tmp location
  data.map(x => if (x.equals(header)) trimQuotedCSVString(x, ",") else x)
    .toDF()
    .repartition(1)
    .write
    .mode(SaveMode.Overwrite)
    .text("data/schema/csvFile")

  //read schema file and infer schema
  val schemaDF = spark.read
    .option("header", "true")
    .option("ignoreLeadingWhiteSpace", "true")
    .option("ignoreTrailingWhiteSpace", "true")
    .option("inferSchema", "true")
    .csv("data/schema/csvFile")

  val schema = schemaDF.schema

  val transfromation = Transfromation(schema)

  val csvDF = spark.read.text("data/input/csvFile")
    .parseCSV()
    .filter($"rawData" !== header)
    .filter($"rawData" !== testRecord)
    .doBasicValidation()
  csvDF.show

  //
  csvDF
    .filter($"errorMessage".isNull)
    .drop($"errorMessage")
    .drop($"rawData")
    .repartition(1)
    .write.mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv("data/output/csvFile")

  csvDF
    .filter($"errorMessage".isNotNull)
    .select($"errorMessage", $"rawData")
    .withColumn("line", concat($"errorMessage", lit(","), $"rawData"))
    .drop($"errorMessage")
    .drop($"rawData")
    .repartition(1)
    .write.mode(SaveMode.Overwrite)
    .text("data/error/file")

}
