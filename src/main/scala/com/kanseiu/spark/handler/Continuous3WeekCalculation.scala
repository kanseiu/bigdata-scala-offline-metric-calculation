package com.kanseiu.spark.handler

import com.kanseiu.spark.common.{ClickHouseConnector, SparkSessionBuilder}
import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters

object Continuous3WeekCalculation {

    def main(args: Array[String]): Unit = {

        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession("Continuous3WeekCalculation")

        import sparkSession.implicits._

        // 指定统计日期（end_date）
        val analysisDateStr = "2022-08-10"
        val splitSymbol = "_"

        // 计算统计周期（date_range）
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val analysisDate = LocalDate.parse(analysisDateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        val startOfWeek = analysisDate.`with`(TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY))
        val endOfWeek = analysisDate.`with`(TemporalAdjusters.nextOrSame(java.time.DayOfWeek.SUNDAY))
        val startDate = startOfWeek.minusWeeks(2).format(formatter)
        val endDate = endOfWeek.format(formatter)

//        println(s"统计周期：${startDate}_${endDate}")

        // 读取指定时间范围内的登录日志数据
        val activeWeeksPerCustomer = sparkSession.sql(s"""
            SELECT
                COUNT(m.unique_weeks) AS active_weeks
            FROM
            (
                SELECT
                    t.customer_id,
                    t.yearWeek AS unique_weeks
                FROM
                (
                    SELECT
                        customer_id,
                        CONCAT(CAST(year(DATE(login_time)) AS STRING), '-', LPAD(CAST(weekofyear(DATE(login_time)) AS STRING), 2, '0')) as yearWeek
                    FROM
                        dwd.log_customer_login
                    WHERE
                        DATE(login_time) BETWEEN '$startDate' AND '$endDate'
                ) t
                GROUP BY
                    t.customer_id, t.yearWeek
            ) m
            GROUP BY
                m.customer_id
            """
        )

        val customersWithThreeActiveWeeks = activeWeeksPerCustomer.filter($"active_weeks" >= 3)

        val activeUserCount = customersWithThreeActiveWeeks.count();

        // 准备写入ClickHouse的数据
        val resultDF = Seq(
            (analysisDate, activeUserCount.toInt, s"$startDate$splitSymbol$endDate")
        ).toDF("end_date", "active_total", "date_range")

        // 写入clickhouse
        ClickHouseConnector.writeToClickHouse(resultDF, "ds_result.continuous_3week")
    }
}