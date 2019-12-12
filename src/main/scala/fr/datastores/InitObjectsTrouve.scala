package fr.datastores

import fr.datastores.helper.spark_extraction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, map}
import org.apache.spark.sql.types.FloatType

object InitObjectsTrouve {

  def main(args: Array[String]): Unit = {
    parseCommand(args) match {
      case Some(parameters) => execute(parameters)
      case None => System.exit(1)
    }
  }

  def execute(parameters:Parameters): Unit = {


    // 0 Extraction de la data depuis fichier CSV pour INIT
    val objectTrouve: DataFrame = spark_extraction.readCSV(parameters.path)
      .select(
      col("Code UIC").as("codeUIC"),
      col("Type d'objets").as("typeObject"),
      col("Nature d'objets").as("nature"),
      col("Gare").as("gare"),
      col("Date").as("date")
    )


    // 1:::: Get information Gares
    val gare_info: DataFrame = spark_extraction.getFromEs("referentiel-gares")
      .withColumn("location",   map(lit("lat"),col("Latitude WGS84").cast(FloatType),lit("lon"),col("Longitude WGS84").cast(FloatType)))
      .select(
        col("location"),
        col("Intitulé gare"),
        col("Commune"),
        col("Code UIC")
      )



    // 2:::: Jointure pour denormalisation
    val dataDenormalise = gare_info.join(objectTrouve, objectTrouve("codeUIC") === gare_info("Code UIC"),"right")


    // 4:::: Insertion dans ElasticSearch
    spark_extraction.saveToEs(dataDenormalise,parameters.index)

  }


  def parseCommand(args:Seq[String]) ={
    val parser = new scopt.OptionParser[Parameters]("Extraction") {
      head("Extraction","0.0.1")
      opt[String]("index") valueName("<Index>") action {(value, parameters) => parameters.copy(index = value)} text ("Nom de l'index")
      opt[String]("path") valueName("Path>") action {(value, parameters) => parameters.copy(path = value)} text ("csv path")
    }
    parser.parse(args,Parameters())
  }

  case class Parameters(index:String = "",path:String = "")

}
