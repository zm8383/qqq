package com.atguigu.gmall.realtime.spark.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall.realtime.spark.util.{MyKafkaSink, MyKafkaUtil, OffsetManagerUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("sparkStreamingDB")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val topic ="ODS_BASE_DB_C"
    val groupId="third"

    val offset: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var kafkaData: InputDStream[ConsumerRecord[String, String]] =null


    if(offset == null){
      kafkaData = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)

    }else{
      kafkaData= MyKafkaUtil.getKafkaStream(topic,ssc,offset,groupId)
    }

//    获取每次偏移量最后的数据
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaData.transform {
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        println(offsetRanges(0).untilOffset + "*****")
        rdd
      }
    }

    val jsonObjData: DStream[JSONObject] = offsetDStream.map(record => {
      JSON.parseObject(record.value())
    })


    val  dimTables=Array("user_info","sku_info","base_province")

    jsonObjData.foreachRDD{rdd=>{


      rdd.foreachPartition{jsonObjItr=>{
        val client: Jedis = RedisUtil.getJedisClient

          for(jsonObj <- jsonObjItr){
            val table: String = jsonObj.getString("table")

            if(dimTables.contains(table)){
//              维度表

              println(jsonObj.toJSONString)
              val pkNameJson: JSONArray = jsonObj.getJSONArray("pkNames")
              val pkNameId: String = pkNameJson.getString(0)
              val jSONArray: JSONArray = jsonObj.getJSONArray("data")
              import scala.collection.JavaConverters._
              for(data <- jSONArray.asScala){
                val jSONObject: JSONObject = data.asInstanceOf[JSONObject]
                val pkValue: String = jSONObject.getString(pkNameId)
                val key="MID:"+table.toUpperCase()+":"+pkValue
                val value=jSONObject.toJSONString
                client.set(key,value)

              }


            }else{
//              事实表

              //            根据每条数据table，type来指定数据放那个topic
              val optType: String = jsonObj.getString("type")
              var opt=""
              if(optType=="INSERT"){
                opt="I"
              }else if (optType=="UPDATE"){
                opt="U"
              }else if (optType=="DELETE"){
                opt="D"
              }

              if(opt.length>0){
                var optTopic="DWD_"+table.toUpperCase+optType+"_"+opt

                val jsonArr: JSONArray = jsonObj.getJSONArray("data")
                import scala.collection.JavaConverters._

                for(data <- jsonArr.asScala){

                  //                把数据发送给kafka
                  MyKafkaSink.send(optTopic,data.toString)
                }
              }
            }

          }

          OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)

        client.close()
      }

        }
      }
    }


    ssc.start()
    ssc.awaitTermination()












  }

}
