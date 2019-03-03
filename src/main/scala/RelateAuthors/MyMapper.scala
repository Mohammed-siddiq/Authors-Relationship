package RelateAuthors

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML


/**
  * parse XML
  * extract the authors
  * if authors are from UIC check if they have co authors
  * ignore if they don't have co-authors
  * If they do emit all the combinations of authors
  * suppose (a1,a2,a3) are co-authors
  * then emit (a1,a2)->1,(a1,a3)->1,(a2,a3)->1,(a3,a1)->1,(a3,a2)->1
  */

class MyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("InputFormat")
  val csProf = conf.getStringList("UIC_CS_PROFESSORS")
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  val one = new IntWritable(1)


  def processXML(xml: String): List[String] = {
    logger.info("The XMl for this mapper : " + xml)

    val proceedings = XML.loadString(xml)

    logger.info("Authors for this mapper:" + proceedings \\ "author")

    val authors = (proceedings \\ "author").map(author => author.text.toLowerCase.trim).toList.sorted

    authors filter csProf.contains
  }

  def generateAuthorMapping(authors: List[String]): List[(String, String)] = {
    authors.combinations(2).map { case Seq(x, y) => (x, y) }.toList
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {


    // Process the XML and extract only the CS authors form the XML input.


    val xmlToProcess =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>"


    val csAuthors = processXML(xmlToProcess)

    logger.info("Generating keys for individual authors" + csAuthors)
    for (author <- csAuthors) context.write(new Text(author), one)

    // Generate all pairs of authors
    val authorMappings = generateAuthorMapping(csAuthors)

    // Mapper writing all pairs of CS authors working on the processed paper
    for (mapping <- authorMappings) {

      logger.info("Generating keys:  ", mapping._1 + "," + mapping._2)

      context.write(new Text(mapping._1 + "," + mapping._2), one)// Emitting the Key value pairs
    }


  }


}
