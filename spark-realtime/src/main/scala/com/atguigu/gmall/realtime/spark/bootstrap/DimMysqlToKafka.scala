package com.atguigu.gmall.realtime.spark.bootstrap

import java.util

import com.alibaba.fastjson.JSONObject
import com.atguigu.gmall.realtime.spark.util.{MyKafkaSink, MysqlUtil}

object DimMysqlToKafka {

  def main(args: Array[String]): Unit = {
    val  dimTables=Array("user_info","sku_info","base_province")

    for (table <- dimTables) {
      val jsonList: util.List[JSONObject] = MysqlUtil.queryList("select * from "+table)

      val jSONObject = new JSONObject()

      jSONObject.put("data",jsonList)
      jSONObject.put("table",table)
      jSONObject.put("type","INSERT")
      jSONObject.put("pkNames",util.Arrays.asList("id"))



      MyKafkaSink.send("ODS_BASE_DB_C",jSONObject.toJSONString)
    }

    MyKafkaSink.close

  }

}
