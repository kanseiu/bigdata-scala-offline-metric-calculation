package com.kanseiu.spark.handler

import com.kanseiu.spark.common.{ClickHouseConnector, SparkSessionBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{rank, round, sum}

object SalesAmountRankCalculation {

    def main(args: Array[String]): Unit = {
        // 创建 sparkSession
        val sparkSession: SparkSession = SparkSessionBuilder.getOrCreateSparkSession("SalesAmountRankCalculation")

        // 读取最新分区的订单主表数据，过滤掉 已退款 的订单
        val factOrderMaster = sparkSession.sql("""
            SELECT
                order_sn
            FROM
                dwd.fact_order_master
            WHERE
                etl_date = (SELECT MAX(etl_date) FROM dwd.fact_order_master)
            AND
                order_status = '已下单'
            AND
                order_sn NOT IN (SELECT order_sn FROM dwd.fact_order_master WHERE etl_date = (SELECT MAX(etl_date) FROM dwd.fact_order_master) AND order_status = '已退款')
            """
        )

        // 读取订单明细表数据（没提到最新分区，因此全部读取）
        val factOrderDetail = sparkSession.sql("""
            SELECT
                order_sn, product_id, product_cnt, product_price
            FROM
                dwd.fact_order_detail
            """
        )

        // 关联订单主表和订单明细表
        val joinedData = factOrderMaster.join(factOrderDetail, "order_sn")

        import sparkSession.implicits._

        // 计算各商品的总销售金额和销售数量
        val salesData = joinedData.groupBy("product_id")
          .agg(
              round(sum($"product_cnt" * $"product_price"), 3).as("sales_amount"),
              sum($"product_cnt").as("product_totalcnt")
          )

        // 计算销售金额排名
        val salesRank = salesData.withColumn("sales_rank", rank().over(Window.orderBy($"sales_amount".desc)))

        // 写入clickhouse
        ClickHouseConnector.writeToClickHouse(salesRank, "ds_result.sales_amount_rank")
    }
}