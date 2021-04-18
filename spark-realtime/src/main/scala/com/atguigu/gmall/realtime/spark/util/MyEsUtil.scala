package com.atguigu.gmall.realtime.spark.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index, Search}

object MyEsUtil {
  private    var  factory:  JestClientFactory=null;

  def getClient:JestClient ={
    if(factory==null)build();
    factory.getObject

  }

  def  build(): Unit ={
    factory=new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(1000).build())

  }

  def main(args: Array[String]): Unit = {

    saveData()
  }



  def saveData(): Unit ={

    val jest: JestClient = getClient
    val index: Index = new Index.Builder(Movie("234","指环王")).index("movie").`type`("_doc").build()

    jest.execute(index)
  }

  case class Movie(id:String,movie_name:String)


//  批量写入
  def saveBulkData(dataList:List[(Any,String)],indexName:String): Unit ={

    if(dataList.size>0 && dataList!=null){

      val jest: JestClient = getClient

      val bulkBuilder = new Bulk.Builder

      for((data,id) <- dataList){
        val index: Index = new Index.Builder(data).index(indexName).`type`("_doc").id(id).build()

        bulkBuilder.addAction(index)

      }

      val bulk: Bulk = bulkBuilder.build()
      jest.execute(bulk)

      jest.close()
    }
  }


    def queryData(): Unit ={

      val jest: JestClient = getClient


    //  new Search.Builder().build()


  }
}
