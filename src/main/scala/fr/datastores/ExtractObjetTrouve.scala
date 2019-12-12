package fr.datastores

import java.text.SimpleDateFormat
import java.util.Calendar

import fr.datastores.helper.{ElasticHelper, spark_extraction}
import fr.datastores.helper.spark_extraction.spark
import fr.datastores.model.ObjetTrouve
import org.apache.spark.sql.{Column, DataFrame}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.map
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.types.{ArrayType, DataType, FloatType}

import scala.io.Source

object ExtractObjetTrouve {

  val now = Calendar.getInstance().getTime()

  def main(args: Array[String]): Unit = {
    parseCommand(args) match {
      case Some(parameters) => execute(parameters)
      case None => System.exit(1)
    }
  }

  def execute(parameters:Parameters): Unit = {

    // 0 :::: Extraction de la data depuis l'API SNCF
    //val url =  "https://data.sncf.com/api/records/1.0/search/?dataset="+parameters.typeObject+"&rows="+parameters.nbRows+"&sort=date&facet=date&refine.date="+parameters.date
    val url =  "https://data.sncf.com/api/records/1.0/search/?dataset=objets-trouves-restitution&rows=1000&sort=date&facet=date&refine.date=2019-12-07"
    val data: String = Source.fromURL(url).mkString
    val fieldsMap: List[Map[String, Any]] = getFields(data)
    val objects: List[ObjetTrouve] = fieldsMap.map(ligne => getObjectTrouve(ligne))


    // 1:::: Get Object perdu ou retroué
    val object_trouve: DataFrame =  spark_extraction.getDataFrame(objects)
    val cleaned = object_trouve
      .withColumn("codeUIC",col("codeUIC").substr(3,20))
      .select(
        col("codeUIC"),
        col("typeObject"),
        col("nature"),
        col("gare"),
        col("date")
      )

    // 2:::: Get information Gares
    val gare_info: DataFrame = spark_extraction.getFromEs("objets_gare_info")
      .withColumn("location",   map(lit("lat"),col("Latitude").cast(FloatType),lit("lon"),col("Longitude").cast(FloatType)))
      .select(
        col("location"),
        col("intitule_gare"),
        col("Commune"),
        col("Code_UIC")
      )

    // 3:::: Jointure pour denormalisation
    val dataDenormalise = gare_info.join(cleaned, cleaned("codeUIC") === gare_info("Code_UIC"))


    // 4:::: Insertion dans ElasticSearch
    spark_extraction.saveToEs(dataDenormalise,"objects_trouves")

  }

  def getFields(data:String) = {
    implicit val formats = org.json4s.DefaultFormats
    val mapJson: Map[String, Any] = parse(data).extract[Map[String,Any]]
    val records: List[Map[String, Any]] = mapJson.get("records").getOrElse(List[Map[String,Any]]()).asInstanceOf[List[Map[String,Any]]]
    records.map(data => data.get("fields").getOrElse(Map[String,Any]()).asInstanceOf[Map[String,Any]])
  }


  def getObjectTrouve(ligne:Map[String,Any]) ={
    ObjetTrouve(ligne.get("date").getOrElse("No").asInstanceOf[String],
      ligne.get("date").getOrElse("No").asInstanceOf[String],
      ligne.get("gc_obo_gare_origine_r_name").getOrElse("No").asInstanceOf[String],
      ligne.get("gc_obo_gare_origine_r_code_uic_c").getOrElse("No").asInstanceOf[String],
      ligne.get("gc_obo_nature_c").getOrElse("No").asInstanceOf[String],
      ligne.get("gc_obo_type_c").getOrElse("No").asInstanceOf[String])
  }


  def getDate() ={
    val minuteFormat = new SimpleDateFormat("YYYY-MM-dd")
    val now = Calendar.getInstance().getTime()
    minuteFormat.format(now)
  }


  def parseCommand(args:Seq[String]) ={
      val parser = new scopt.OptionParser[Parameters]("Extraction") {
        head("Extraction","0.0.1")
        opt[String]("type-object") valueName("<Type Object>") action {(value, parameters) => parameters.copy(typeObject = value)} text ("objets-trouves-restitution or objets-trouves-gares")
        opt[String]("index") valueName("<Index>") action {(value, parameters) => parameters.copy(index = value)} text ("Index ES dans lequel les données seront inserées")
        opt[Int]("nbRows") valueName("<nbRows>") action {(value, parameters) => parameters.copy(nbRows = value)} text ("nombre de documents retourné par l'API (Default 1000)")
        opt[String]("date") valueName("<date") action {(value, parameters) => parameters.copy(date = value)} text ("Date à laquelle les objets ont été trouve ou perdu. (Default today)")
      }
    parser.parse(args,Parameters())
  }

  case class Parameters(typeObject:String = "",index:String = "",nbRows:Int = 1000, date:String = getDate())
}