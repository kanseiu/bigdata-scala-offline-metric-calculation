package com.kanseiu.spark.handler

import com.kanseiu.spark.common.{ClickHouseConnector, SparkSessionBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{rank, round}

object PaymentCVRCalculation {

    def main(args: Array[String]): Unit = {
        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession("PaymentCVRCalculation")

        import sparkSession.implicits._

        // 读取Hive中的订单数据，并计算各省已下单的数量和已退款的数量，
        // 假设order_status包含已下单和已退款状态，
        // 即：fact_order_master 中，一个订单可能有多条记录
        // 已支付的订单数 = 已下单数 - 已退款数
        // 比如有20条已下单的记录，但又有10条已退款的记录，只有下单的记录才会有退款记录，则实际支付的订单数为10
        val orders = sparkSession.sql("""
            SELECT
                province,
                SUM(case when order_status = '已下单' then 1 else 0 end) as create_order,
                SUM(case when order_status = '已退款' then 1 else 0 end) as refund_order
            FROM
                dwd.fact_order_master
            WHERE
                YEAR(create_time) = 2022
            GROUP BY
                province
            """
        )

        // 计算支付转化率
        val conversionRate = orders.withColumn("payCVR", round(($"create_order" - $"refund_order") / $"create_order", 3))

        // 使用窗口函数计算排名
        import org.apache.spark.sql.expressions.Window
        val windowSpec = Window.orderBy($"payCVR".desc)
        val rankedConversionRate = conversionRate.withColumn("ranking", rank().over(windowSpec))

        // 写入clickhouse
        ClickHouseConnector.writeToClickHouse(rankedConversionRate, "ds_result.payment_cvr")
    }
}