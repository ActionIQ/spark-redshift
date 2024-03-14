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
         |day_of_week varchar(100),
         |str_not_null varchar(100) not null
         |)
      """.stripMargin
    )
    // scalastyle:off
    conn.createStatement().executeUpdate(
      s"""
         |insert into $tableName values
         |('ASDF', 1706565058123, 1706590812001, 'EST', 'yyyy/MM/ddThh:mm:ss', '2024-01-29T16:50:58.123', 'Mon', 'abc'),
         |('blah', 1706565058001, 1706478758001, 'Asia/Shanghai', 'yyyy-MM-dd hh:mm:ss.SSS', '2024-01-30T05:50:58.001', 'WED', 'def'),
         |(null, 1718841600000, 1704085146456, 'America/New_York', 'yyyy-MM-dd HH', '2024-06-19T20:00:00.000', 'sun', 'XYZ'),
         |('x', 1718841600000, 1704085146456, 'UTC', 'yyyy-MM-dd', '2024-06-20T00:00:00.000', 'SUN', 'boo')
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

  test("null safe equal pushdown") {
    val filteredDf = read.option("dbtable", testTable).load().where("lower(teststring) <=> 'asdf'")
    assert(filteredDf.count() === 1L)
    checkPlan(filteredDf.queryExecution.executedPlan) { sql =>
      sql.contains(
        "CASE WHEN ( LOWER ( SUBQUERY_0.teststring ) = 'asdf' ) THEN true  " +
          "WHEN ( ( LOWER ( SUBQUERY_0.teststring ) IS NULL ) AND ( 'asdf' IS NULL ) ) THEN true " +
          "ELSE false END"
      )
    }
  }

  test("approx_count_distinct pushdown") {
    val df = read.option("dbtable", testTable).load().selectExpr("approx_count_distinct(teststring)")
    checkOneCol(df, Seq(3L))
    checkPlan(df.queryExecution.sparkPlan) { sql =>
      sql.contains("APPROXIMATE COUNT ( DISTINCT SUBQUERY_1.SUBQUERY_1_COL_0 )")
    }
  }

  test("instr pushdown") {
    val df = read.option("dbtable", testTable).load().selectExpr("instr(teststring, 'a')")
    checkOneCol(df, Seq(0, 3, null, 0))
    checkPlan(df.queryExecution.executedPlan) { sql =>
      sql.contains("POSITION ( 'a' IN SUBQUERY_0.teststring )")
    }
  }

  test("sha2 pushdown") {
    val df = read.option("dbtable", testTable).load().selectExpr("sha2(teststring, 256)")
    checkOneCol(df, Seq(
      "99b3bcf690e653a177c602dd9999093b9eb29e50a3af9a059af3fcbfab476a16",
      "8b7df143d91c716ecfa5fc1730022f6b421b05cedee8fd52b1fc65a96030ad52",
      null,
      "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"))
    checkPlan(df.queryExecution.executedPlan) { sql =>
      sql.contains(
        "SHA2 ( CAST ( CAST ( SUBQUERY_0.teststring AS VARBINARY ) AS VARCHAR ) , 256 )"
      )
    }
  }

  test("concat and concat_ws pushdown") {
    val table = read.option("dbtable", testTable).load()
    val df1 = table.selectExpr("concat(teststring, 'blah', teststring)")
    checkOneCol(df1, Seq("ASDFblahASDF", "blahblahblah", null, "xblahx"))
    checkPlan(df1.queryExecution.executedPlan) { sql =>
      sql.contains("CONCAT ( SUBQUERY_0.teststring , CONCAT ( 'blah' , SUBQUERY_0.teststring ) )")
    }

    val df2 = table.selectExpr("concat_ws('-', teststring, str_not_null, 'blah')")
    checkOneCol(df2, Seq("ASDF-abc-blah", "blah-def-blah", "XYZ-blah", "x-boo-blah"))
    checkPlan(df2.queryExecution.executedPlan) { sql =>
      sql.contains("CONCAT ( CASE WHEN ( SUBQUERY_0.teststring IS NOT NULL ) THEN CONCAT ( SUBQUERY_0.teststring , '-' ) ELSE '' END , CONCAT ( CONCAT ( SUBQUERY_0.str_not_null , '-' ) , 'blah' )")
    }

    val df3 = table.selectExpr("concat_ws(teststring, 'abc', 'xyz', 'BOO')")
    checkOneCol(df3, Seq("abcASDFxyzASDFBOO", "abcblahxyzblahBOO", null, "abcxxyzxBOO"))
    checkPlan(df3.queryExecution.executedPlan) { sql =>
      sql.contains("CONCAT ( 'abc' , CONCAT ( SUBQUERY_0.teststring , CONCAT ( 'xyz' , CONCAT ( SUBQUERY_0.teststring , NVL2 ( SUBQUERY_0.teststring , 'BOO' , NULL ) ) ) ) )")
    }

    val df4 = table.selectExpr("concat_ws('-', coalesce(str_not_null, 'BOO'), 'foo', 'bar')")
    checkOneCol(df4, Seq("abc-foo-bar", "def-foo-bar", "XYZ-foo-bar", "boo-foo-bar"))
    checkPlan(df4.queryExecution.executedPlan) { sql =>
      sql.contains("CONCAT ( SUBQUERY_0.str_not_null , CONCAT ( '-' , CONCAT ( 'foo' , CONCAT ( '-' , 'bar' ) ) ) )")
    }

    val df5 = table.selectExpr("concat_ws('-', 'blah', str_not_null)")
    checkOneCol(df5, Seq("blah-abc", "blah-def", "blah-XYZ", "blah-boo"))
    checkPlan(df5.queryExecution.executedPlan) { sql =>
      sql.contains("CONCAT ( 'blah' , CONCAT ( '-' , SUBQUERY_0.str_not_null ) )")
    }
  }

  test("string reverse pushdown") {
    val df = read.option("dbtable", testTable).load().selectExpr("reverse(teststring)")
    checkOneCol(df, Seq("FDSA", "halb", null, "x"))
    checkPlan(df.queryExecution.executedPlan) { sql =>
      sql.contains("REVERSE")
    }
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

  test("aiq_day_start pushdown") {
    val table = read.option("dbtable", testTable).load()
    val df1 = table.selectExpr("aiq_day_start(ts_ms_2, 'America/New_York', 180)")
    checkOneCol(df1, Seq(1722139200000L, 1721966400000L, 1719547200000L, 1719547200000L))
    checkPlan(df1.queryExecution.executedPlan) { sql =>
      // https://gist.github.com/dorisZ017/0afe86d57a5c14982757fb166d5e9077
      sql.contains("EXTRACT ( 'epoch' FROM CONVERT_TIMEZONE ( 'America/New_York' , 'UTC' , DATE_TRUNC (") &&
        sql.contains("DATEADD ( day, 180")
    }
    val df2 = table.selectExpr("aiq_day_start(ts_ms_2, timezone, -1)")
    checkOneCol(df2, Seq(1706504400000L, 1706371200000L, 1703912400000L, 1703980800000L))
    checkPlan(df2.queryExecution.executedPlan) { sql =>
      sql.contains("DATEADD ( day, -1")
    }
  }

}
