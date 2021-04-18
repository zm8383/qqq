package com.atguigu.gmall.realtime.spark.util

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}
import java.util

import com.alibaba.fastjson.JSONObject



object MysqlUtil {
  def main(args: Array[String]): Unit = {
    val list: util.List[JSONObject] = queryList("select * from activity_sku")
    println(list)
  }

  def queryList(sql: String): util.List[JSONObject] = {
    Class.forName("com.mysql.jdbc.Driver")
    val resultList: util.List[JSONObject] = new util.ArrayList[JSONObject]()
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall2020?characterEncoding=utf-8&useSSL=false", "root", "root")
    val stat: Statement = conn.createStatement
    println(sql)
    val rs: ResultSet = stat.executeQuery(sql)
    val md: ResultSetMetaData = rs.getMetaData
    while (rs.next) {
      val rowData = new JSONObject();
      for (i <- 1 to md.getColumnCount) {
        rowData.put(md.getColumnName(i), rs.getObject(i))
      }
      resultList.add(rowData)
    }

    stat.close()
    conn.close()
    resultList


  }
}
