package fr.datastores.helper

import fr.datastores.model.ObjetTrouve
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object spark_extraction {

  val spark = SparkSession
    .builder
    .master("local[*]")
    .config("es.nodes","188.165.208.58")
    .config("es.port","31728")
    .config("es.index.auto.create","true")
    .config("es.nodes.wan.only","true")
    .appName("Extraction")
    .getOrCreate()

  import spark.implicits._

  def getDataFrame(data:Seq[ObjetTrouve]) = {
    data.toDF
  }


  def getFromEs(index:String) ={
    spark.read.format("org.elasticsearch.spark.sql").option("es.resource",index).load()
  }

  def readCSV(path:String) ={
    spark.read
      .option("delimiter",";")
      .option("header","true")
      .option("inferSchema","true")
      .csv(path)
  }


  def saveToEs(df:DataFrame,index:String): Unit ={
    df.write
      .format("org.elasticsearch.spark.sql")
      .option("es.resource",index)
      .mode(SaveMode.Overwrite)
      .save()
  }
}
