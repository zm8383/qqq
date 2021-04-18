package com.atguigu.gmall.realtime.spark.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManagerUtil {


  def saveOffset(topic:String,group:String,offsetRanges: Array[OffsetRange]): Unit ={
    val client: Jedis = RedisUtil.getJedisClient

    val key=topic+":"+group
    val hashMap = new util.HashMap[String,String]()

    for(offsetRange <- offsetRanges){

      val partition: String = offsetRange.partition.toString
      val untilOffset: String = offsetRange.untilOffset.toString

      hashMap.put(partition,untilOffset)

      println("偏移量分区:"+partition+":"+offsetRange.fromOffset.toString+"-->"+untilOffset)
    }

    client.hmset(key,hashMap)

    client.close()

  }


  def getOffset(topic:String,group:String):Map[TopicPartition,Long] ={

    val client: Jedis = RedisUtil.getJedisClient

    val key=topic+":"+group

    val partitionToOffset: util.Map[String, String] = client.hgetAll(key)

    client.close()
    import scala.collection.JavaConverters._

    val offsetMap: Map[TopicPartition, Long] = partitionToOffset.asScala.map {
      case (partition, offset) => {
        val topicPartition = new TopicPartition(topic, partition.toInt)
        val offsetLong: Long = offset.toLong
        (topicPartition, offsetLong)
      }
    }.toMap

    offsetMap

  }

}
