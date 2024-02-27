package com.kanseiu.spark.common

import org.apache.spark.sql.{DataFrame, SaveMode}

import java.util.Properties

object ClickHouseConnector {

    // 获取clickhouse 配置
    private def getClickHouseProperties: Properties = {
        val properties = new Properties()
        properties.setProperty("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        properties.setProperty("user", "default")
        properties.setProperty("password", "123456")
        properties
    }

    // 使用公共方法获取连接属性，并执行数据写入操作
    def writeToClickHouse(df: DataFrame, tableName: String): Unit = {
        val clickHouseUrl = "jdbc:clickhouse://master:8123/ds_result"
        val properties = getClickHouseProperties

        df.write
          .mode(SaveMode.Overwrite)
          .jdbc(clickHouseUrl, tableName, properties)
    }
}