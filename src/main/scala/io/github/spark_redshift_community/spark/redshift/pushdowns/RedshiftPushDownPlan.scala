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
// https://github.com/ActionIQ-OSS/spark-snowflake/blob/spark_2.4/src/main/scala/net/snowflake/spark/snowflake/pushdowns/SnowflakePlan.scala
// scalastyle:on line.size.limit

package io.github.spark_redshift_community.spark.redshift.pushdowns

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{StructField, StructType}

case class RedshiftPushDownPlan(output: Seq[Attribute], rdd: RDD[InternalRow]) extends SparkPlan {
  override def children: Seq[SparkPlan] = Nil
  protected override def doExecute(): RDD[InternalRow] = {

    val schema = StructType(
      output.map(attr => StructField(attr.name, attr.dataType, attr.nullable))
    )

    rdd.mapPartitions { iter =>
      val project = UnsafeProjection.create(schema)
      iter.map(project)
    }
  }
}
