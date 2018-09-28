package rj.spark.data.pipeline.hr.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import rj.spark.data.pipeline.hr.app.Dataloader.schema
import rj.spark.data.pipeline.hr.util.Utility.{booleanCleanup, numberCleanup, trimQuotedString}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Helper object to do CSV validation and cleanup by input schema
  *
  * @author rjain
  **/

class Transfromation(val schema: StructType) {

}

object Transfromation {

  //Extra Columns
  final val ERROR_COL = "errorMessage";
  final val RAWDATA_COL = "rawData";
  //create extended Schema using below supporting columns
  final val newSchema = schema
    .add(ERROR_COL, StringType)
    .add(RAWDATA_COL, StringType)
  final val ERROR_COL_INDEX = newSchema.fieldIndex((ERROR_COL));
  final val RAWDATA_COL_INDEX = newSchema.fieldIndex((RAWDATA_COL));
  //Create String version of schema for firs pass
  final val schemaWithString = new StructType(newSchema.fields.map(x => new StructField(x.name, StringType)))

  //get Original Schema
  def apply(schema: StructType) = new Transfromation(schema);

  //Seq Extension to supper insert,update at particular position and truncate to a size
  implicit class SeqExtension[T](val seq: Seq[T]) extends AnyVal {
    def append(value: T) = {
      seq ++ Seq(value)
    }

    def insert(i: Int, values: Seq[T]) = {
      if (seq.size > i) {
        val (front, back) = seq.splitAt(i)
        front ++ values ++ back
      }
      else {
        seq ++ values
      }
    }

    def update(i: Int, values: Seq[T]) = {
      if (seq.size > i) {
        val (front, back) = seq.zipWithIndex.filter(_._2 != i).map(_._1).splitAt(i)
        front ++ values ++ back
      } else {
        seq.zipWithIndex.filter(_._2 != i).map(_._1) ++ values
      }
    }

    def truncate(i: Int) = {
      if (seq.size > i) {
        seq.zipWithIndex.filter(_._2 >= i - 1).map(_._1)
      } else {
        seq
      }
    }
  }

  //all Data Transforms implicate utility
  implicit class DataFrameTransforms(df: DataFrame) extends Serializable {

    //parseCSV
    def parseCSV(): DataFrame = {
      val noCol = newSchema.fieldNames.length
      val resultRdd: RDD[Row] = df.rdd.map { row: Row => {
        val line = Option(row.getAs[String](0))
        Row.fromSeq(
          (if (line.isDefined && !line.get.startsWith("#")) {
            var values = Utility.trimQuotedCSV(line.get)
            newSchema.fields.map(x => {
              val colIndex = newSchema.fieldIndex(x.name)
              val value = Option(Try(values(colIndex))
              match {
                case Success(v) => v
                case Failure(ex) => null.asInstanceOf[String]
              })
              x.dataType
              match {
                case IntegerType | FloatType | LongType | DoubleType => values = values.update(colIndex, Utility.numberSplit(value.getOrElse("0")))
                case _ => if (value.isDefined) value.get else null.asInstanceOf[String]
              }
            })
            val size = values.size;
            if (size < noCol) {
              for (i <- size to ERROR_COL_INDEX) yield
                values = values.insert(i, Seq(null.asInstanceOf[String]))
              if (size < ERROR_COL_INDEX)
                values = values.update(ERROR_COL_INDEX, Seq("\"The number of columns in the record doesn't match file header spec.\""))
            }
            else if (size > noCol)
              values = values.truncate(noCol)
            values = values.update(RAWDATA_COL_INDEX, Seq(line.get))
            values
          }
          else {
            new Array[String](noCol).toSeq
          }).toSeq)
      }
      }
      return df.sparkSession.createDataFrame(resultRdd, schemaWithString)
    }

    /*
       Apply basic clean rules and then schema validation based in input newSchema type
     */
    def doBasicValidation(): DataFrame = {

      if (!df.schema.fieldNames.mkString(",").equals(newSchema.fieldNames.mkString(",")))
        throw new RuntimeException("Input Schema fields [" + newSchema.fieldNames.mkString(",") + "], doesn't match with DataFrame Schema fields [" + df.schema.fieldNames.mkString(",") + "]")
      val resultRdd: RDD[Row] = df.rdd.map { row: Row => {
        val errorMap: mutable.Set[String] = new mutable.HashSet[String]()
        val record = newSchema.fields.map(x => {
          val value = row.getAs[String](x.name)
          Try(
            x.dataType
            match {
              case StringType => if (!(x.name.equals(ERROR_COL) || x.name.equals(RAWDATA_COL))) trimQuotedString(Utility.nvl(value, null.asInstanceOf[String])) else value
              case IntegerType => numberCleanup(Utility.nvl(value, "0")).toInt
              case FloatType => numberCleanup(Utility.nvl(value, "0")).toFloat
              case LongType => numberCleanup(Utility.nvl(value, "0")).toLong
              case DoubleType => numberCleanup(Utility.nvl(value, "0")).toDouble
              //                 case DateType => if(value.isDefined) value.get else null.asInstanceOf[Int] //todo date conversion
              //                 case TimestampType => if(value.isDefined) value.get.toInt else null.asInstanceOf[Int] //todo date timestamp conversion
              case BooleanType => booleanCleanup(Utility.nvl(value, null.asInstanceOf[String])).toBoolean
              //               case _ => throw new UnsupportedOperationException() //todo other data type validation
            }
          ) match {
            case Success(cleanValue) => (row.fieldIndex(x.name), if (Option(cleanValue).isDefined) cleanValue.toString else null.asInstanceOf[String])
            case Failure(ex) => {
              errorMap.+=(x.name)
              (row.fieldIndex(x.name), value)
            }
          }
        })

        val dataMap = record
          .map(x => if (x._1 == ERROR_COL_INDEX && !Option(x._2).isDefined && errorMap.nonEmpty)
            (x._1, "\"The datatypes of columns:" + errorMap.mkString("[", ",", "]") + " doesn't match the datatypes specified in the first test record.\"")
          else
            (x._1, x._2))

        val newRow = Row.fromSeq(
          dataMap.toSeq
            .sortWith(_._1 < _._1)
            .map(_._2))
        newRow
      }
      }
      return df.sparkSession.createDataFrame(resultRdd, df.schema)
    }
  }

}
