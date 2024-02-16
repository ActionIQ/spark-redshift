package io.github.spark_redshift_community.spark.redshift

import io.github.spark_redshift_community.spark.redshift.pushdowns.RedshiftPushDownPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.DataFrame

// scalastyle:off line.size.limit
class AiqRedshiftSuite extends IntegrationSuiteBase {
  private val testTable: String = "aiq_read_test_table"

  override def createTestDataInRedshift(tableName: String): Unit = {
    conn.createStatement().executeUpdate(
      s"""
         |create table $tableName (
         |teststring varchar(256),
         |ts_ms int8,
         |timezone varchar(50),
         |fmt varchar(100),
         |ts_str varchar(100)
         |)
      """.stripMargin
    )
    // scalastyle:off
    conn.createStatement().executeUpdate(
      s"""
         |insert into $tableName values
         |('ASDF', 1706565058123, 'EST', 'yyyy/MM/ddThh:mm:ss', '2024-01-29T16:50:58.123'),
         |('blah', 1706565058001, 'Asia/Shanghai', 'yyyy-MM-dd hh:mm:ss.SSS', '2024-01-30T05:50:58.001'),
         |(null, 1718841600000, 'America/New_York', 'yyyy-MM-dd HH', '2024-06-19T20:00:00.000'),
         |('x', 1718841600000, 'UTC', 'yyyy-MM-dd', '2024-06-20T00:00:00.000')
         |""".stripMargin
    )
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    conn.prepareStatement(s"drop table if exists $testTable").executeUpdate()
    createTestDataInRedshift(testTable)
  }

  override def afterAll(): Unit = {
    // scalastyle:off
    // verify all executed queries were tagged
    assert(RedshiftPushDownSqlStatement.capturedQueries.forall(_.startsWith("/* partner:partner,testing:true */\nUNLOAD")))
    // scalastyle:on
    try {
      conn.prepareStatement(s"drop table if exists $testTable").executeUpdate()
    } finally {
      super.afterAll()
    }
  }

  private def checkPlan(plan: SparkPlan)(check: String => Boolean): Unit = {
    plan match {
      case RedshiftPushDownPlan(output, rdd, sql) =>
        assert(check(sql), s"SQL did not match: ${sql}")
      case p => assert(false, s"$p is not a RedshiftPushDownPlan")
    }
  }

  private def checkOneCol[T](df: DataFrame, expected: Seq[T], colId: Int = 0): Unit = {
    val actual = df.collect().toSeq.map(_.getAs[T](colId))
    assert(actual.size == expected.size && actual.toSet == expected.toSet,
      s"expected: ${expected} and actual: ${actual} don't match"
    )
  }


  test("Pushdown lower") {
    val table = read.option("dbtable", testTable).load()
    val df = table.where("lower(teststring) = 'asdf'")
    checkPlan(df.queryExecution.sparkPlan) { sql =>
      sql.contains("LOWER ( SUBQUERY_0.teststring ) = 'asdf'")
    }
    val cnt = df.count()
    assert(cnt == 1L)
  }

  test("Pushdown limit and take") {
    val table = read.option("dbtable", testTable).load()
    val df1 = table.select("teststring")
    assert(df1.take(1).length === 1L)
    assert(RedshiftPushDownSqlStatement.capturedQueries.exists(_.contains("LIMIT 1")))

    table.createOrReplaceTempView("test_table")
    val df2 = sqlContext.sql("select teststring from test_table limit 3")
    assert(df2.count() === 3L)
    assert(RedshiftPushDownSqlStatement.capturedQueries.exists(_.contains("LIMIT 3")))
  }


  test("aiq_date_to_string pushdown") {
    val table = read.option("dbtable", testTable).load()
    val df1 = table.selectExpr(
      "aiq_date_to_string(ts_ms, 'yyyy/MM/ddTHH:mm:ss.sss')"
    )
    checkOneCol(
      df1, Seq("2024/01/29T16:50:58.123", "2024/01/29T16:50:58.001", "2024/06/19T20:00:00.000", "2024/06/19T20:00:00.000")
    )
    checkPlan(df1.queryExecution.executedPlan) { sql =>
      /*
      TO_CHAR (
        CONVERT_TIMEZONE (
         'UTC' ,
         'America/New_York' ,
         TIMESTAMP'epoch' + ( ( CAST ( SUBQUERY_0.ts_ms AS FLOAT ) / 1000 ) * INTERVAL '1 SECOND' ) ) ,
       'yyyy/MM/dd"T"HH24:MI:ss.MS' )
       */
      sql.contains("TO_CHAR ( CONVERT_TIMEZONE ( 'UTC' , 'America/New_York'") &&
        sql.contains("'yyyy/MM/dd\"T\"HH24:MI:ss.MS'")
    }

    val df2 = table.selectExpr(
      "aiq_date_to_string(ts_ms, 'yyyy-MM-dd HH:mm:ss.sss', timezone)"
    )
    checkOneCol(
      df2, Seq("2024-01-29 16:50:58.123", "2024-01-30 05:50:58.001", "2024-06-19 20:00:00.000", "2024-06-20 00:00:00.000")
    )
    checkPlan(df2.queryExecution.executedPlan) { sql => sql.contains("TO_CHAR ( CONVERT_TIMEZONE") }

    // format not foldable, cannot pushdown
    val df3 = table.selectExpr("aiq_date_to_string(ts_ms, fmt, timezone)")
    checkOneCol(
      df3, Seq("2024/01/29T04:50:58", "2024-01-30 05:50:58.001", "2024-06-19 20", "2024-06-20")
    )
  }

  test("aiq_string_to_date pushdown") {
    val table = read.option("dbtable", testTable).load()
    val df1 = table.selectExpr(
      "aiq_string_to_date(ts_str, 'yyyy-MM-ddTHH:mm:ss.SSS', 'America/New_York')"
    )
    checkOneCol(
      df1, Seq(1706565058123L, 1706611858001L, 1718841600000L, 1718856000000L)
    )
    checkPlan(df1.queryExecution.executedPlan) { sql =>
      /*
      CAST (
        EXTRACT (
          'epoch'
          FROM
            CONVERT_TIMEZONE (
              'America/New_York',
              'UTC',
              CAST (
                TO_TIMESTAMP (
                  SUBQUERY_0.ts_str, 'yyyy-MM-dd"T"HH24:MI:ss.MS'
                ) AS TIMESTAMP
              )
            )
        ) AS BIGINT
      ) * 1000
      ) + CAST (
        EXTRACT (
          ms
          FROM
            CONVERT_TIMEZONE (
              'America/New_York',
              'UTC',
              CAST (
                TO_TIMESTAMP (
                  SUBQUERY_0.ts_str, 'yyyy-MM-dd"T"HH24:MI:ss.MS'
                ) AS TIMESTAMP
              )
            )
        ) AS BIGINT
      */
      sql.contains("CAST ( EXTRACT ( 'epoch' FROM CONVERT_TIMEZONE ( 'America/New_York' , 'UTC' , CAST ( TO_TIMESTAMP") &&
        sql.contains("+ CAST ( EXTRACT ( ms FROM CONVERT_TIMEZONE ( 'America/New_York' , 'UTC' , CAST ( TO_TIMESTAMP") &&
        sql.contains("yyyy-MM-dd\"T\"HH24:MI:ss.MS'")
    }
    val df2 = table.selectExpr(
      "aiq_string_to_date(ts_str, 'yyyy-MM-ddTHH:mm:ss.SSS', timezone)"
    )
    checkOneCol(
      df2, Seq(1706565058123L, 1706565058001L, 1718841600000L, 1718841600000L)
    )
    checkPlan(df2.queryExecution.executedPlan) { sql =>
      sql.contains("CAST ( EXTRACT ( 'epoch' FROM CONVERT_TIMEZONE ( SUBQUERY_0.timezone , 'UTC' , CAST ( TO_TIMESTAMP ( SUBQUERY_0.ts_str") &&
        sql.contains("+ CAST ( EXTRACT ( ms FROM CONVERT_TIMEZONE ( SUBQUERY_0.timezone , 'UTC' , CAST ( TO_TIMESTAMP")
    }
  }
}
