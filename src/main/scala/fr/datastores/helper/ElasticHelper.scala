package fr.datastores.helper

import fr.datastores.model.ObjetTrouve
import jp.co.bizreach.elasticsearch4s._
import jp.co.bizreach.elasticsearch4s.retry.{FixedBackOff, RetryConfig}

import scala.collection.immutable
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
case class Tweet(name: String, message: String)
case class TweetMessage(message: String)


object ElasticHelper {

  ESClient.init()
  val client = ESClient("http://188.165.208.58:31728")

  implicit val DefaultRetryConfig = RetryConfig(0, Duration.Zero, FixedBackOff)
  val httpClient =  HttpUtils.createHttpClient()

  // "objets_trouves"
  def es_bulkload(data:List[ObjetTrouve],index:String): Unit ={
    val config = ESConfig(index)
    val bulk: immutable.Seq[BulkAction.Index] = for (elem <- data) yield BulkAction.Index(config,elem)
    client.bulk(bulk)
  }


  def es_bulkGet() ={
    val config = ESConfig("objets_trouves")

//    client.list(config){ builder =>
//        builder.query(matchPhraseQuery("codeUIC", "0087773002"))
//      }


   // client.search(config).

    val test: String = HttpUtils.post(httpClient, "http://188.165.208.58:31728/objets_perdus/_search/",s"""
         |{
         |  "query": {
         |    "bool": {
         |      "filter": [
         |        {
         |          "bool": {
         |            "should": [
         |              {
         |                "match": {
         |                  "codeUIC": "0087773002"
         |                }
         |              }
         |            ],
         |            "minimum_should_match": 1
         |          }
         |        }
         |      ],
         |      "should": [],
         |      "must_not": []
         |    }
         |  }
         |}
         |""".stripMargin)

    httpClient.close()

    println(test)

   /* val data = client.find[ObjetTrouve](config){ builder =>
      builder.query(idsQuery().addIds("1234"))
    }*/

    //data
  }

  def shutdown(): Unit ={
    ESClient.shutdown()
  }


  def main(args: Array[String]): Unit = {
    val test = es_bulkGet()
    //println(test.source.size)
  }

}
