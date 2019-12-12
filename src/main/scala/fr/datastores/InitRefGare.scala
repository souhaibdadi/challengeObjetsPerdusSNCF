package fr.datastores

import fr.datastores.helper.spark_extraction
import org.apache.spark.sql.DataFrame

object InitRefGare {

  def main(args: Array[String]): Unit = {
    parseCommand(args) match {
      case Some(parameters) => execute(parameters)
      case None => System.exit(1)
    }
  }

  def execute(parameters:Parameters): Unit = {

    // 0 Extraction de la data depuis fichier CSV pour INIT
    val objectTrouve: DataFrame = spark_extraction.readCSV(parameters.path)


    // 4:::: Insertion dans ElasticSearch
    spark_extraction.saveToEs(objectTrouve,parameters.index)
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
