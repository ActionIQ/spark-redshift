/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.spark_redshift_community.spark.redshift.pushdowns.querygeneration

import org.apache.spark.sql.catalyst.expressions.{Add, AddMonths, AiqDateToString, AiqDayDiff, AiqDayOfTheWeek, AiqDayStart, AiqStringToDate, AiqWeekDiff, Attribute, CaseWhen, Cast, ConvertTimezone, DateAdd, DateDiff, DateFormatClass, DateSub, Divide, Expression, Extract, Floor, In, Literal, Lower, MakeInterval, MillisToTimestamp, Month, Multiply, ParseToTimestamp, Quarter, Subtract, TruncDate, TruncTimestamp, UnixMillis, Upper, Year}
import io.github.spark_redshift_community.spark.redshift._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{DoubleType, LongType, NullType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

/** Extractor for datetime expressions */
private[querygeneration] object DateStatement {
  // DateAdd's pretty name in Spark is "date_add",
  // the counterpart's name in SF is "DATEADD".
  // And the syntax is some different.
  val DATEADD = "DATEADD"

  def unapply(
    expAttr: (Expression, Seq[Attribute])
  ): Option[RedshiftPushDownSqlStatement] = {
    val expr = expAttr._1
    val fields = expAttr._2

    Option(expr match {
      case DateAdd(startDate, days) =>
        ConstantString(DATEADD) +
          blockStatement(
            ConstantString("day,") +
              convertStatement(days, fields) + "," +
              convertStatement(startDate, fields)
          )

      // To be tested
      case DateSub(startDate, days) =>
        ConstantString(DATEADD) +
          blockStatement(
            ConstantString("day, (0 - (") +
              convertStatement(days, fields) + ") )," +
              convertStatement(startDate, fields)
          )

      // Add AddMonths() support here
      // But, Spark 3.0, it doesn't. For example,
      // On spark 2.3/2.4, "2015-02-28" +1 month -> "2015-03-31"
      // On spark 3.0,     "2015-02-28" +1 month -> "2015-03-28"
      case AddMonths(_, _) => null

      case _: Month | _: Quarter | _: Year |
           _: TruncDate | _: TruncTimestamp =>
        ConstantString(expr.prettyName.toUpperCase) +
          blockStatement(convertStatements(fields, expr.children: _*))

      case ParseToTimestamp(left, Some(fmt), _, None) if fmt.foldable =>
        val fmtExpr = validFmtExpr(fmt)
        functionStatement("TO_TIMESTAMP", Seq(left, fmtExpr).map(convertStatement(_, fields)))

      case ConvertTimezone(sourceTz, targetTz, sourceTs) =>
        functionStatement(
          "CONVERT_TIMEZONE", Seq(sourceTz, targetTz, sourceTs).map(convertStatement(_, fields))
        )
        
      case Extract(field, source, _) if field.foldable =>
        val fieldStr = field.eval().toString
        val sourceStmt = convertStatement(source, fields)
        functionStatement(
          "EXTRACT",
          Seq(ConstantString(fieldStr) + "FROM" + sourceStmt)
        )

      case MakeInterval(years, months, weeks, days, hours, mins, secs, _) =>
        def helper(expr: Expression, dtPart: String): Option[RedshiftPushDownSqlStatement] = {
          val stmt = if (expr.foldable) {
            // handles null
            Option(expr.eval()).collect {
              case v if v.toString.toDouble != 0.0 => ConstantString(v.toString).toStatement
            }
          } else { Option(convertStatement(expr, fields)) }

          // e.g. SELECT 2 * INTERVAL '1 SECOND'
          stmt.map(s => s + "*" + s"INTERVAL '1 $dtPart'")
        }
        // days + 7 * weeks if (weeks is not null or 0)
        val totalDays = {
          if (weeks.foldable) {
            val weekNum = Option(expr.eval()).fold(0)(_.toString.toInt)
            if (weekNum != 0) {
              // no need to add to statements if it's 0
              Add(Multiply(Literal(weekNum), Literal(7)), days)
            } else { days }
          } else { Add(Multiply(weeks, Literal(7)), days) }
        }
        val stmtParts = Seq(
          helper(years, "YEAR"),
          helper(months, "MONTH"),
          helper(totalDays, "DAY"),
          helper(hours, "HOUR"),
          helper(mins, "MINUTE"),
          helper(secs, "SECOND")
        ).collect { case Some(stmt) => stmt }
        blockStatement(mkStatement(stmtParts, "+"))

      case MillisToTimestamp(child) =>
      // https://stackoverflow.com/a/64656770
      // TIMESTAMP'epoch' + ms * INTERVAL'0.001 SECOND'
        ConstantString("TIMESTAMP'epoch' +") +
          convertStatement(child, fields) + "* INTERVAL'0.001 SECOND'"

      case UnixMillis(child) =>
        // CAST ( EXTRACT ('epoch' from ts) AS BIGINT) * 1000 +
        // CAST ( EXTRACT (ms from ts) AS BIGINT)
        val secsExpr = Multiply(
          Cast(Extract(Literal("'epoch'"), child, Literal.default(LongType)), LongType),
          Literal(1000L)
        )
        val msExpr = Cast(Extract(Literal("ms"), child, Literal.default(LongType)), LongType)
        convertStatement(Add(secsExpr, msExpr), fields)

      case AiqDateToString(ts, fmt, tz) if fmt.foldable =>
        convertStatement(
          DateFormatClass(
            ConvertTimezone(Literal("UTC"), tz, MillisToTimestamp(ts)),
            validFmtExpr(fmt)
          ),
          fields
        )

      case DateDiff(endDate, startDate) =>
        // https://docs.aws.amazon.com/redshift/latest/dg/r_DATEDIFF_function.html
        functionStatement(
          "DATEDIFF",
          ConstantString("days").toStatement
            +: Seq(startDate, endDate).map(convertStatement(_, fields))
        )

      case AiqStringToDate(tsStr, fmt, tz) if fmt.foldable =>
        val localTsExpr = Cast(
          ParseToTimestamp(tsStr, Option(fmt), StringType),
          TimestampType
        )
        val msExpr = UnixMillis(
          ConvertTimezone(tz, Literal("UTC"), localTsExpr)
        )
        convertStatement(msExpr, fields)

      case DateFormatClass(timestamp, fmt, None) =>
        functionStatement(
          "TO_CHAR",
          Seq(timestamp, fmt).map(convertStatement(_, fields))
        )

      case AiqDayDiff(startTs, endTs, timezoneId) =>
        val Seq(startDt, endDt) = Seq(startTs, endTs).map { ts =>
          ConvertTimezone(Literal("UTC"), timezoneId, MillisToTimestamp(ts))
        }
        convertStatement(DateDiff(endDt, startDt), fields)

      case AiqWeekDiff(startTs, endTs, startDay, timezoneId) =>
        val startDayNumber = {
          if (startDay.foldable) {
            Literal(
              DateTimeUtils.getAiqDayOfWeekFromString(startDay.eval().asInstanceOf[UTF8String])
            )
          } else {
            // org.apache.spark.sql.catalyst.util.DateTimeUtils.getAiqDayOfWeekFromString
            CaseWhen(Seq(
              ("SU", "SUN", "SUNDAY", 4),
              ("MO", "MON", "MONDAY", 3),
              ("TU", "TUE", "TUESDAY", 2),
              ("WE", "WED", "WEDNESDAY", 1),
              ("TH", "THU", "THURSDAY", 0),
              ("FR", "FRI", "FRIDAY", 6),
              ("SA", "SAT", "SATURDAY", 5)
            ).map { case (nameAlt1, nameAlt2, nameAlt3, value) =>
              (
                In(Upper(startDay),
                  Seq(Literal(nameAlt1), Literal(nameAlt2), Literal(nameAlt3))),
                Literal(value)
              )
            })
          }
        }
        val Seq(startWeeksSinceEpoch, endWeeksSinceEpoch) = Seq(startTs, endTs).map { ms =>
          val daysSinceEpoch = AiqDayDiff(Literal(0), ms, timezoneId)
          Floor(Divide(Add(daysSinceEpoch, startDayNumber), Literal(7)))
        }
        convertStatement(Subtract(endWeeksSinceEpoch, startWeeksSinceEpoch), fields)

      case AiqDayOfTheWeek(epochTimestamp, timezoneId) =>
        // format timestamp with day of week pattern
        convertStatement(
          Lower(
            DateFormatClass(
              ConvertTimezone(Literal("UTC"), timezoneId, MillisToTimestamp(epochTimestamp)),
              validFmtExpr(Literal("EEEE"))
            )
          ),
          fields
        )

      case AiqDayStart(timestamp, timezone, plusDays) =>
        val dateExpr = UnixMillis(
          ConvertTimezone(
            timezone,
            Literal("UTC"),
            TruncTimestamp(
              Literal("DAY"),
              DateAdd(
                ConvertTimezone(Literal("UTC"), timezone, MillisToTimestamp(timestamp)),
                plusDays
              )
            )
          )
        )
        convertStatement(dateExpr, fields)

      case _ => null
    })
  }

  private def validFmtExpr(fmt: Expression): Expression = {
    // https://docs.aws.amazon.com/redshift/latest/dg/r_FORMAT_strings.html
    val validFmt = fmt.eval().toString
      .replaceAll("([^'])T([^'])", "$1\"T\"$2")
      .replaceAll("HH", "HH24")
      .replaceAll("mm", "MI")
      .replaceAll("a", "AM")
      .replaceAll(".sss|.SSS", ".MS")
      .replaceAll("EEEE", "FMDay")
    Literal(validFmt)
  }
}
