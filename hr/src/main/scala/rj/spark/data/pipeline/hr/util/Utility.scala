package rj.spark.data.pipeline.hr.util

import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object Utility {
  final val BOOLEAN_PATTERN = "(YES|NO|Y|N|1|TRUE|FALSE)+".r
  val trimQuotedStr = udf { s: String => trimQuotedString(s) }

  def nvl(s: String, d: String): String = {
    if (Option(s).isDefined && !s.isEmpty)
      s
    else
      d
  }

  def trimQuotedCSVString(s: String, d: String): String = {
    trimQuotedCSV(s)
      .mkString("\"", "\"" + d + "\"", "\"")
  }

  def trimQuotedCSV(s: String): Seq[String] = {
    s.split((",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(trimQuotedString).toSeq
  }

  def trimQuotedString(s: String): String = {
    if (s == null)
      return null
    s.trim.replaceAll("(^\"|\"$)", "").trim
  }

  def numberSplit(s: String): Seq[String] = {
    if (Option(s).isDefined)
      trimQuotedString(s.replaceAll("\"", "")).split("\\s+").toSeq
    else
      Seq[String](s)
  }

  def numberCleanup(s: String): String = {
    if (Option(s).isDefined)
      trimQuotedString(s.replaceAll("\\s*\\.*$", ""))
    else
      s
  }

  def booleanCleanup(s: String): String = {
    if (Option(s).isDefined) {
      val BOOLEAN_PATTERN(b) = s
      return Try(
        b.toBoolean.toString
      ) match {
        case Success(value) => value
        case Failure(ex) => null
      }
    }
    else
      null
  }

}
