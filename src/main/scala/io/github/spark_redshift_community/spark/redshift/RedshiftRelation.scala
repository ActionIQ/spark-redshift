/*
 * Copyright 2015 TouchType Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.spark_redshift_community.spark.redshift

import java.io.InputStreamReader
import java.net.URI
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.eclipsesource.json.Json
import io.github.spark_redshift_community.spark.redshift.Parameters.MergedParameters
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Data Source API implementation for Amazon Redshift database tables
 */
private[redshift] case class RedshiftRelation(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentialsProvider => AmazonS3Client,
    params: MergedParameters,
    userSchema: Option[StructType],
    jdbcOptions: JDBCOptions)
    (@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  private val log = LoggerFactory.getLogger(getClass)

  private val creds = AWSCredentialsUtils.load(params, sqlContext.sparkContext.hadoopConfiguration)

  if (sqlContext != null) {
    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)
  }

  private val tableNameOrSubquery =
    params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get

  override lazy val schema: StructType = {
    userSchema.getOrElse {
      val tableNameOrSubquery =
        params.query.map(q => s"($q)").orElse(params.table.map(_.toString)).get
      val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
      try {
        jdbcWrapper.resolveTable(conn, tableNameOrSubquery)
      } finally {
        conn.close()
      }
    }
  }

  override def toString: String = s"RedshiftRelation($tableNameOrSubquery)"

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val saveMode = if (overwrite) {
      SaveMode.Overwrite
    } else {
      SaveMode.Append
    }
    val writer = new RedshiftWriter(jdbcWrapper, s3ClientFactory)
    writer.saveToRedshift(sqlContext, data, saveMode, params)
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filterNot(filter => FilterPushdown.buildFilterExpression(schema, filter).isDefined)
  }

  private def executeCountQuery(query: String): RDD[InternalRow] = {
    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
    val queryWithTag = RedshiftPushDownSqlStatement.appendTagsToQuery(jdbcOptions, query)
    try {
      val results = jdbcWrapper.executeQueryInterruptibly(conn.prepareStatement(queryWithTag))
      if (results.next()) {
        val numRows = results.getLong(1)
        val parallelism = sqlContext.getConf("spark.sql.shuffle.partitions", "200").toInt
        val emptyRow = RowEncoder(StructType(Seq.empty)).toRow(Row(Seq.empty))
        sqlContext.sparkContext
          .parallelize(1L to numRows, parallelism)
          .map(_ => emptyRow)

      } else {
        throw new IllegalStateException("Could not read count from Redshift")
      }
    } finally {
      conn.close()
    }
  }

  private def executeUnloadQuery(query: String,
                                 schema: StructType
                                ): RDD[InternalRow] = {
    // Unload data from Redshift into a temporary directory in S3:
    val tempDir = params.createPerQueryTempDir()
    val unloadSql = buildUnloadStmt(query, tempDir, creds, params.sseKmsKey)
    val conn = jdbcWrapper.getConnector(params.jdbcDriver, params.jdbcUrl, params.credentials)
    try {
      jdbcWrapper.executeInterruptibly(conn.prepareStatement(unloadSql))
    } finally {
      conn.close()
    }
    // Read the MANIFEST file to get the list of S3 part files that were written by Redshift.
    // We need to use a manifest in order to guard against S3's eventually-consistent listings.
    val filesToRead: Seq[String] = {
      val cleanedTempDirUri =
        Utils.fixS3Url(Utils.removeCredentialsFromURI(URI.create(tempDir)).toString)
      val s3URI = Utils.createS3URI(cleanedTempDirUri)
      val s3Client = s3ClientFactory(creds)
      val is = s3Client.getObject(s3URI.getBucket, s3URI.getKey + "manifest").getObjectContent
      val s3Files = try {
        val entries = Json.parse(new InputStreamReader(is)).asObject().get("entries").asArray()
        entries.iterator().asScala.map(_.asObject().get("url").asString()).toSeq
      } finally {
        is.close()
      }
      // The filenames in the manifest are of the form s3://bucket/key, without credentials.
      // If the S3 credentials were originally specified in the tempdir's URI, then we need to
      // reintroduce them here
      s3Files.map { file =>
        tempDir.stripSuffix("/") + '/' + file.stripPrefix(cleanedTempDirUri).stripPrefix("/")
      }
    }

    sqlContext.read
      .format(classOf[RedshiftFileFormat].getName)
      .schema(schema)
      .option("nullString", params.nullString)
      .load(filesToRead: _*)
      .queryExecution.executedPlan.execute()
  }
  /**
   * Check if S3 and redshift are in same region
   * Check if S3 has life cycle configured
   */
  private def checkS3Setting(): Unit = {
    for (
      redshiftRegion <- Utils.getRegionForRedshiftCluster(params.jdbcUrl);
      s3Region <- Utils.getRegionForS3Bucket(params.rootTempDir, s3ClientFactory(creds))
    ) {
      if (redshiftRegion != s3Region) {
        // We don't currently support `extraunloadoptions`, so even if Amazon _did_ add a `region`
        // option for this we wouldn't be able to pass in the new option. However, we choose to
        // err on the side of caution and don't throw an exception because we don't want to break
        // existing workloads in case the region detection logic is wrong.
        log.error("The Redshift cluster and S3 bucket are in different regions " +
          s"($redshiftRegion and $s3Region, respectively). Redshift's UNLOAD command requires " +
          s"that the Redshift cluster and Amazon S3 bucket be located in the same region, so " +
          s"this read will fail.")
      }
    }
    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))
  }

  def buildRDDFromSQL(statement: RedshiftPushDownSqlStatement,
                      schema: StructType): RDD[InternalRow] = {
    val queryString = statement.toString
    log.info(s"Executing query with extra pushdown ${queryString} in redshift")
    if (queryString.contains("COUNT(")) {
      executeCountQuery(queryString)
    } else {
      checkS3Setting()
      val escapedQuery = queryString.replace("\\", "\\\\").replace("'", "\\'")
      executeUnloadQuery(escapedQuery, schema)
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter])
  : RDD[Row] = {
    checkS3Setting()
    if (requiredColumns.isEmpty) {
      // In the special case where no columns were requested, issue a `count(*)` against Redshift
      // rather than unloading data.
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val countQuery = s"SELECT count(*) FROM $tableNameOrSubquery $whereClause"
      log.info(countQuery)
      executeCountQuery(countQuery).asInstanceOf[RDD[Row]]
    } else {
      assert(!requiredColumns.isEmpty)
      val columnList = requiredColumns.map(col => s""""$col"""").mkString(", ")
      val whereClause = FilterPushdown.buildWhereClause(schema, filters)
      val query = {
        // Since the query passed to UNLOAD will be enclosed in single quotes, we need to escape
        // any backslashes and single quotes that appear in the query itself
        val escapedTableNameOrSubqury = tableNameOrSubquery.replace("\\", "\\\\").replace("'", "\\'")
        s"SELECT $columnList FROM $escapedTableNameOrSubqury $whereClause"
      }
      val prunedSchema = pruneSchema(schema, requiredColumns)
      executeUnloadQuery(query, prunedSchema).asInstanceOf[RDD[Row]]
    }
  }

  override def needConversion: Boolean = false

  private def buildUnloadStmt(
      query: String,
      tempDir: String,
      creds: AWSCredentialsProvider,
      sseKmsKey: Option[String]): String = {
    val credsString: String =
      AWSCredentialsUtils.getRedshiftCredentialsString(params, creds.getCredentials)
    // We need to remove S3 credentials from the unload path URI because they will conflict with
    // the credentials passed via `credsString`.
    val fixedUrl = Utils.fixS3Url(Utils.removeCredentialsFromURI(new URI(tempDir)).toString)

    val sseKmsClause = sseKmsKey.map(key => s"KMS_KEY_ID '$key' ENCRYPTED").getOrElse("")
    val unloadQuery = s"UNLOAD ('$query') TO '$fixedUrl' WITH CREDENTIALS '$credsString'" +
      s" ESCAPE MANIFEST NULL AS '${params.nullString}'" +
      s" $sseKmsClause"
    val finalQuery = RedshiftPushDownSqlStatement.appendTagsToQuery(jdbcOptions, query)
    log.info(finalQuery)
    finalQuery
  }

  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }
}
