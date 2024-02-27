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
         |ts_ms_2 int8,
         |timezone varchar(50),
         |fmt varchar(100),
         |ts_str varchar(100),
         |day_of_week varchar(100)
         |)
      """.stripMargin
    )
    // scalastyle:off
    conn.createStatement().executeUpdate(
      s"""
         |insert into $tableName values
         |('ASDF', 1706565058123, 1706590812001, 'EST', 'yyyy/MM/ddThh:mm:ss', '2024-01-29T16:50:58.123', 'Mon'),
         |('blah', 1706565058001, 1706478758001, 'Asia/Shanghai', 'yyyy-MM-dd hh:mm:ss.SSS', '2024-01-30T05:50:58.001', 'WED'),
         |(null, 1718841600000, 1704085146456, 'America/New_York', 'yyyy-MM-dd HH', '2024-06-19T20:00:00.000', 'sun'),
         |('x', 1718841600000, 1704085146456, 'UTC', 'yyyy-MM-dd', '2024-06-20T00:00:00.000', 'SUN')
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
          'UTC', 'America/New_York', TIMESTAMP 'epoch' + SUBQUERY_0.ts_ms * INTERVAL '0.001 SECOND'
        ),
        'yyyy/MM/dd"T"HH24:MI:ss.MS'
      )
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

  test("aiq_day_diff pushdown") {
    val table = read.option("dbtable", testTable).load()
    val df1 = table.selectExpr("aiq_day_diff(ts_ms, ts_ms_2, timezone)")
    checkOneCol(df1, Seq(1, -1, -171, -171))
    checkPlan(df1.queryExecution.executedPlan) { sql =>
      sql.contains("DATEDIFF ( days , CONVERT_TIMEZONE ( 'UTC' , SUBQUERY_0.timezone")
    }
    val df2 = table.selectExpr("aiq_day_diff(1704085146456, ts_ms, 'America/New_York')")
    checkOneCol(df2, Seq(29, 29, 171, 171))
    checkPlan(df2.queryExecution.executedPlan) { sql =>
      /*
      DATEDIFF (
        days,
        CONVERT_TIMEZONE (
          'UTC', 'America/New_York', TIMESTAMP 'epoch' + 1704085146456 * INTERVAL '0.001 SECOND'
        ),
        CONVERT_TIMEZONE (
          'UTC', 'America/New_York', TIMESTAMP 'epoch' + SUBQUERY_0.ts_ms * INTERVAL '0.001 SECOND'
        )
      )
       */
      sql.contains("DATEDIFF ( days , CONVERT_TIMEZONE ( 'UTC' , 'America/New_York'")
    }
  }

  test("aiq_week_diff pushdown") {
    val table = read.option("dbtable", testTable).load()
    val df1 = table.selectExpr("aiq_week_diff(1705507488000, ts_ms, 'sun', 'America/New_York')")
    checkOneCol(df1, Seq(2, 2, 22, 22))
    checkPlan(df1.queryExecution.executedPlan) { sql =>
      // https://gist.github.com/dorisZ017/672a12e209611e4698bd4ab9f76d2a9a#file-aiq_week_diff_rsft_1-sql
      sql.contains("+ 4 ) / 7 )") && sql.contains("FLOOR")
    }
    val df2 = table.selectExpr("aiq_week_diff(1705507488000, ts_ms, day_of_week, 'America/New_York')")
    checkOneCol(df2, Seq(2, 1, 22, 22))
    checkPlan(df2.queryExecution.executedPlan) { sql =>
      // https://gist.github.com/dorisZ017/672a12e209611e4698bd4ab9f76d2a9a#file-aiq_week_diff_rsft_2-sql
      sql.contains("+ CASE WHEN UPPER ( SUBQUERY_0.day_of_week ) IN ( 'SU' , 'SUN' , 'SUNDAY' ) THEN 4")
    }
    // bad non-foldable start day
    val df3 = table.selectExpr("aiq_week_diff(1705507488000, ts_ms, timezone, 'America/New_York')")
    checkOneCol(df3, Seq(null, null, null, null))
    checkPlan(df3.queryExecution.executedPlan) { sql =>
      sql.contains("+ CASE WHEN UPPER ( SUBQUERY_0.timezone ) IN ( 'SU' , 'SUN' , 'SUNDAY' ) THEN 4")
    }
    val df4 = table.selectExpr("aiq_week_diff(1705507488000, ts_ms_2, 'wed', timezone)")
    checkOneCol(df4, Seq(2, 1, -3, -3))
    checkPlan(df4.queryExecution.executedPlan) { sql =>
      sql.contains("+ 1 ) / 7 )") && sql.contains("FLOOR")
    }
  }

  test("aiq_day_of_the_week pushdown") {
    val table = read.option("dbtable", testTable).load()
    val df1 = table.selectExpr("aiq_day_of_the_week(ts_ms_2, 'Asia/Shanghai')")
    checkOneCol(df1, Seq("tuesday", "monday", "monday", "monday"))
    checkPlan(df1.queryExecution.executedPlan) { sql =>
      /*
      LOWER (
        TO_CHAR (
          CONVERT_TIMEZONE (
            'UTC', 'Asia/Shanghai', TIMESTAMP 'epoch' + SUBQUERY_0.ts_ms_2 * INTERVAL '0.001 SECOND'
          ),
          'FMDay'
        )
      )
       */
      sql.contains("( LOWER ( TO_CHAR ( CONVERT_TIMEZONE ( 'UTC' , 'Asia/Shanghai'") &&
        sql.contains("'FMDay'")
    }
    val df2 = table.selectExpr("aiq_day_of_the_week(ts_ms_2, timezone)")
    checkOneCol(df2, Seq("tuesday", "monday", "sunday", "monday"))
    checkPlan(df2.queryExecution.executedPlan) { sql =>
      sql.contains("( LOWER ( TO_CHAR ( CONVERT_TIMEZONE ( 'UTC' , SUBQUERY_0.timezone") &&
        sql.contains("'FMDay'")
    }
  }
}
