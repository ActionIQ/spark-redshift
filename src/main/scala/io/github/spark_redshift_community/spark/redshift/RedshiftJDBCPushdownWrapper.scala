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

// scalastyle:off line.size.limit
// https://github.com/ActionIQ-OSS/spark-snowflake/blob/spark_2.4/src/main/scala/net/snowflake/spark/snowflake/SnowflakeJDBCWrapper.scala
// scalastyle:on line.size.limit

package io.github.spark_redshift_community.spark.redshift

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

private[redshift] case class ConstantString(override val value: String)
  extends StatementElement

private[redshift] object ConstantStringVal {
  def apply(l: Any): StatementElement = {
    if (l == null || l.toString.toLowerCase == "null") {
      ConstantString("NULL")
    } else {
      ConstantString(l.toString)
    }
  }
}

sealed trait StatementElement {

  val value: String

  val isVariable: Int = 0

  def +(element: StatementElement): RedshiftPushDownSqlStatement =
    new RedshiftPushDownSqlStatement(
      isVariable + element.isVariable,
      element :: List[StatementElement](this)
    )

  def +(statement: RedshiftPushDownSqlStatement): RedshiftPushDownSqlStatement =
    new RedshiftPushDownSqlStatement(
      isVariable + statement.numOfVar,
      statement.list ::: List[StatementElement](this)
    )

  def +(str: String): RedshiftPushDownSqlStatement = this + ConstantString(str)

  override def toString: String = value

  def ! : RedshiftPushDownSqlStatement = toStatement

  def toStatement: RedshiftPushDownSqlStatement =
    new RedshiftPushDownSqlStatement(isVariable, List[StatementElement](this))

  def sql: String = value
}

object RedshiftPushDownSqlStatement {
  // FOR TESTING ONLY!
  val capturedQueries = ListBuffer.empty[String]

  private[redshift] def appendTagsToQuery(jdbcOptions: JDBCOptions, query: String): String = {
    val jdbcProps = jdbcOptions.asProperties.asScala
    val aiqPropsString = jdbcProps.collect {
      case (k, v) if k.startsWith("aiq_") =>
        k.replace("aiq_", "") + ":" + v
    }.mkString(",")

    val finalQuery = if (aiqPropsString.nonEmpty) {
      s"/* $aiqPropsString */\n$query"
    } else {
      query
    }

    if (jdbcProps.get("aiq_testing").exists(_.toBoolean)) {
      RedshiftPushDownSqlStatement.capturedQueries.append(finalQuery)
    }
    finalQuery
  }
}

// scalastyle:off
// https://stackoverflow.com/questions/2265503/why-do-i-need-to-override-the-equals-and-hashcode-methods-in-java
private[redshift] class RedshiftPushDownSqlStatement(
  val numOfVar: Int = 0,
  val list: List[StatementElement] = Nil
) extends Serializable {
  def +(element: StatementElement): RedshiftPushDownSqlStatement =
    new RedshiftPushDownSqlStatement(numOfVar + element.isVariable, element :: list)

  def +(statement: RedshiftPushDownSqlStatement): RedshiftPushDownSqlStatement =
    new RedshiftPushDownSqlStatement(
      numOfVar + statement.numOfVar,
      statement.list ::: list
    )

  def +(str: String): RedshiftPushDownSqlStatement = this + ConstantString(str)

  def isEmpty: Boolean = list.isEmpty

  override def equals(obj: scala.Any): Boolean =
    obj match {
      case other: RedshiftPushDownSqlStatement =>
        if (this.statementString == other.statementString) true else false
      case _ => false
    }

  override def toString: String = {
    val buffer = new StringBuilder
    val sql = list.reverse

    sql.foreach {
      case x: ConstantString =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x)
      case x: VariableElement[_] =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x.sql)
    }

    buffer.toString()
  }

  def statementString: String = {
    val buffer = new StringBuilder
    val sql = list.reverse

    sql.foreach {
      case x: ConstantString =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x)
      case x: VariableElement[_] =>
        if (buffer.nonEmpty && buffer.last != ' ') {
          buffer.append(" ")
        }
        buffer.append(x.value)
        buffer.append("(")
        buffer.append(x.variable)
        buffer.append(")")
    }

    buffer.toString()
  }
}
// scalastyle:on

private[redshift] sealed trait VariableElement[T] extends StatementElement {
  override val value = "?"

  override val isVariable: Int = 1

  val variable: Option[T]

  override def sql: String = if (variable.isDefined) variable.get.toString else "NULL"

}

private[redshift] case class Identifier(name: String)
  extends VariableElement[String] {
  override val variable = Some(name)
  override val value: String = "identifier(?)"
}

private[redshift] case class StringVariable(override val variable: Option[String])
  extends VariableElement[String] {
  override def sql: String = if (variable.isDefined) s"""'${variable.get}'""" else "NULL"
}

private[redshift] case class IntVariable(override val variable: Option[Int])
  extends VariableElement[Int]

private[redshift] case class LongVariable(override val variable: Option[Long])
  extends VariableElement[Long]

private[redshift] case class ShortVariable(override val variable: Option[Short])
  extends VariableElement[Short]

private[redshift] case class FloatVariable(override val variable: Option[Float])
  extends VariableElement[Float]

private[redshift] case class DoubleVariable(override val variable: Option[Double])
  extends VariableElement[Double]

private[redshift] case class BooleanVariable(override val variable: Option[Boolean])
  extends VariableElement[Boolean]

private[redshift] case class ByteVariable(override val variable: Option[Byte])
  extends VariableElement[Byte]