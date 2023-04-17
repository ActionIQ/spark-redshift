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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PythonUDF, ScalaUDF}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF

import io.github.spark_redshift_community.spark.redshift.RedshiftPushDownSqlStatement

private[querygeneration] object UnsupportedStatement {
  /** Used mainly by QueryGeneration.convertStatement. This matches
  * a tuple of (Expression, Seq[Attribute]) representing the expression to
  * be matched and the fields that define the valid fields in the current expression
  * scope, respectively.
  *
  * @param expAttr A pair-tuple representing the expression to be matched and the
  *                attribute fields.
  * @return An option containing the translated SQL, if there is a match, or None if there
  *         is no match.
  */
  def unapply(
               expAttr: (Expression, Seq[Attribute])
             ): Option[RedshiftPushDownSqlStatement] = {
    val expr = expAttr._1

    // This exception is not a real issue. It will be caught in
    // QueryBuilder. It can't be done here
    // because it is not clear whether there are any cloud tables here.
    throw new IllegalArgumentException(
      "failed to pushdown")
  }

  // Determine whether the unsupported operation is known or not.
  private def isKnownUnsupportedOperation(expr: Expression): Boolean = {
    // The pushdown for UDF is known unsupported
    (expr.isInstanceOf[PythonUDF]
      || expr.isInstanceOf[ScalaUDF]
      || expr.isInstanceOf[ScalaUDAF])
  }
}
