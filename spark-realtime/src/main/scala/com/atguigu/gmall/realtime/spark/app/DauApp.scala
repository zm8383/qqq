package com.atguigu.gmall.realtime.spark.app


import java.lang
import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.spark.bean.DauInfo
import com.atguigu.gmall.realtime.spark.util.{MyEsUtil, MyKafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


object DauApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("dauapp")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topic="ODS_BASE_LOG"
    val groupid="third"

    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupid)

    //从kafka接受数据

//    提交offset
//    判断offset是否有值，有，就用偏移量的方法，没有用默认
    var kafkaData: InputDStream[ConsumerRecord[String, String]] =null
    if(offset==null){

       kafkaData= MyKafkaUtil.getKafkaStream(topic,ssc,groupid)
    }else{

      kafkaData= MyKafkaUtil.getKafkaStream(topic,ssc,offset,groupid)
    }

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaData.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "*****")
        rdd
      }
    }


    //    kafkaData.map(_.value()).print()
//    处理 0数据整理  1筛选 2去重

    val handleMap: DStream[JSONObject] = offsetDStream.map(
      record => {
//        0数据整理
        val parseObject: JSONObject = JSON.parseObject(record.value())
        val data: lang.Long = parseObject.getLong("ts")
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        val strTime: String = dateFormat.format(new java.util.Date(data))
        val timeArr: Array[String] = strTime.split(" ")
        parseObject.put("day", timeArr(0))
        parseObject.put("hour", timeArr(1))
        parseObject
      }
    )
//       1筛选出首页的数据
    val filterData: DStream[JSONObject] = handleMap.filter(
      JsonObj => {
        var flag = false
        val pageJson: JSONObject = JsonObj.getJSONObject("page")
        if (pageJson != null) {
          val pageid = pageJson.getString("last_page_id")
          if (pageid == null || pageid.length == 0) {
            flag = true
          }
        }
        flag
      }
    )

//    filterData.print(100)
//     2去重
   /* filterData.filter(
      jsonObj=>{
        val jedis: Jedis = RedisUtil.getJedisClient

        val mid: String = jsonObj.getJSONObject("common").getString("mid")

        val strTime: String = jsonObj.getString("day")
        val dauKey="Dau:"+strTime

        val nonExists: lang.Long = jedis.sadd(dauKey,mid)
        jedis.expire(dauKey,24*3600)
        jedis.close()
        if(nonExists==1L){
          true
        }else{

          false
        }

      }
    )*/

    val result: DStream[JSONObject] = filterData.mapPartitions { iter => {
      val jedis: Jedis = RedisUtil.getJedisClient
      val sourceList: List[JSONObject] = iter.toList
      val dauData = new ListBuffer[JSONObject]()
      println("未筛选前：" + sourceList.size)
      for (jsonObj <- sourceList) {
        val mid: String = jsonObj.getJSONObject("common").getString("mid")

        val strTime: String = jsonObj.getString("day")
        val dauKey = "Dau:" + strTime

        val nonExists: lang.Long = jedis.sadd(dauKey, mid)
        jedis.expire(dauKey, 24 * 3600)

        if (nonExists == 1L) {
          dauData.append(jsonObj)
        } else {
          false
        }
      }

      jedis.close()
      println("筛选后：" + dauData.size)
      dauData.toIterator

    }
    }
    //   输出数据
    result.cache()
    result.print(100)


    val esData: DStream[DauInfo] = result.map(
      jsonObj => {
        val commonObj = jsonObj.getJSONObject("common")

        DauInfo(commonObj.getString("mid"),
          commonObj.getString("uid"),
          commonObj.getString("ar"),
          commonObj.getString("ch"),
          commonObj.getString("vc"),
          jsonObj.getString("dt"),
          jsonObj.getString("hr"),
          System.currentTimeMillis())

      }
    )


    esData.foreachRDD{
      rdd=>{
        rdd.foreachPartition(dauiter=>{

          val fo = new SimpleDateFormat("yyyy-MM-dd")

          val dt: String = fo.format(new java.util.Date())
//          添加ID，幂等性处理
          val tuples: List[(DauInfo, String)] = dauiter.toList.map(DauInfo=>(DauInfo,DauInfo.mid))

          MyEsUtil.saveBulkData(tuples,"gmall_dau_info"+dt)

        })

        OffsetManagerUtil.saveOffset(topic,groupid,offsetRanges)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
