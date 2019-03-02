package RelateAuthors

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML

class MyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  //  val conf = ConfigFactory.load("InputFormat.conf")
  //  val doc = conf.getString("DOC_DTD")
  /**
    * parse XML
    * extract the authors
    * if authors are from UIC check if they have co authors
    * ignore if they don't have co-authors
    * If they do emit all the combinations of authors
    * suppose (a1,a2,a3) are co-authors
    * then emit (a1,a2),(a1,a3),(a2,a3),(a3,a1),(a3,a2)
    */

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("InputFormat")
  val csProf = conf.getStringList("UIC_CS_PROFESSORS")
  val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  val one = new IntWritable(1)


  def processXML(xml: String): List[String] = {
    //    print("The XMl for this mapper :" xml)
    logger.info("The XMl for this mapper : " + xml)
    //    logger.debug("The XMl for this mapper : " + xml)
//    val csProf = List("Balajee Vamanan", "John Bell", "Isabel Cruz", "Bhaskar DasGupta", "Cody Cranch", "Nasim Mobasheri", "Piotr Gmytrasiewicz", "Ugo Buy", "Peter Nelson", "Robert Sloan", "Mark Grechanik", "V. N. Venkatakrishnan", "Anastasios Sidiropoulos", "Jon Solworth", "Elena Zheleva", "Lenore Zuck", "Chris Kanich", "Ajay Kshemkalyani", "Luc Renambot", "Brian Ziebart", "Prasad Sistla", "Gonzalo Bello", "Ian Kash", "Natalie Parde", "Xinhua Zhang", "Robert Kenyon", "Bing Liu", "Jason Polakis", "Evan McCarty", "Ouri Wolfson", "Cornelia Caragea", "Barbara Di Eugenio", "Philip S. Yu", "Emanuelle Burton", "G. Elisabeta Marai", "Andrew Johnson", "William Mansky", "Dale Reed", "Brent Stephens", "Scott Reckinger", "Joe Hummel", "Shanon Reckinger", "Patrick Troy", "Tanya Berger-Wolf", "David Hayes", "Xiaorui Sun", "Jakob Eriksson", "Xingbo Wu", "John Lillis", "Mitchell Theys", "Debaleena Chattopadhyay", "Daniel J. Bernstein").map(_.toLowerCase)
    val proceedings = XML.loadString(xml)
    logger.info("Authors for this mapper:" + proceedings \\ "author")
    val authors = (proceedings \\ "author").map(author => author.text.toLowerCase).toList
    authors filter csProf.contains
  }

  def generateAuthorMapping(authors: List[String]): List[(String, String)] = {
    authors.combinations(2).map { case Seq(x, y) => (x, y) }.toList
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {


    /*
    <inproceedings mdate="2017-05-24" key="conf/icst/GrechanikHB13">
<author>Mark Grechanik</author>
<author>B. M. Mainul Hossain</author>
<author>Ugo Buy</author>
<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>
<pages>174-183</pages>
<year>2013</year>
<booktitle>ICST</booktitle>
<ee>https://doi.org/10.1109/ICST.2013.19</ee>
<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>
<crossref>conf/icst/2013</crossref>
<url>db/conf/icst/icst2013.html#GrechanikHB13</url>
</inproceedings>
     */


    // Process the XML and extract only the CS authors form the XML input.


    val xmlToProcess =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dtdFilePath">
              <dblp>""" + value.toString + "</dblp>"

    //    val xmlToProcess = start_text + value.toString + end_text

    val csAuthors = processXML(xmlToProcess)

    // Generate all pairs of authors
    val authorMappings = generateAuthorMapping(csAuthors)

    // Mapper writing all pairs of CS authors working on the processed paper
    for (mapping <- authorMappings) {
      //      val aKey = new AuthorKey(new Text(mapping._1), new Text(mapping._2))
      //      println("writing Key: ", aKey)
      logger.info("Generating keys:  ", mapping._1 + "-" + mapping._2)
      context.write(new Text(mapping._1 + "-" + mapping._2), one)
      context.write(new Text(mapping._1), one)
      context.write(new Text(mapping._2), one)
    }


  }

  //  @Test
  //  def test(): Unit = {
  //    val csProf = List("Balajee Vamanan", "John Bell", "Isabel Cruz", "Bhaskar DasGupta", "Cody Cranch", "Nasim Mobasheri", "Piotr Gmytrasiewicz", "Ugo Buy", "Peter Nelson", "Robert Sloan", "Mark Grechanik", "V. N. Venkatakrishnan", "Anastasios Sidiropoulos", "Jon Solworth", "Elena Zheleva", "Lenore Zuck", "Chris Kanich", "Ajay Kshemkalyani", "Luc Renambot", "Brian Ziebart", "Prasad Sistla", "Gonzalo Bello", "Ian Kash", "Natalie Parde", "Xinhua Zhang", "Robert Kenyon", "Bing Liu", "Jason Polakis", "Evan McCarty", "Ouri Wolfson", "Cornelia Caragea", "Barbara Di Eugenio", "Philip S. Yu", "Emanuelle Burton", "G. Elisabeta Marai", "Andrew Johnson", "William Mansky", "Dale Reed", "Brent Stephens", "Scott Reckinger", "Joe Hummel", "Shanon Reckinger", "Patrick Troy", "Tanya Berger-Wolf", "David Hayes", "Xiaorui Sun", "Jakob Eriksson", "Xingbo Wu", "John Lillis", "Mitchell Theys", "Debaleena Chattopadhyay", "Daniel J. Bernstein")
  //    //        val path = "PUBLIC \"-//DBLP//DTD//EN\" \n  \"/Users/mohammedsiddiq/IdeaProjects/TestScala/src/resources/dblp.dtd\""
  //
  //
  //
  //    val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
  //
  //    //      val start_text = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\" standalone=\"no\"?>\n<!DOCTYPE dblp " + path + ">\n<dblp>\n"
  //    //        val start_text = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n<!DOCTYPE dblp " + path + ">\n<dblp>\n"
  ////    val start_text = "<?xml version=\"1.0\"?>\n<!DOCTYPE dblp " +  + ">\n<dblp>\n"
  ////    val end_text = "\n</dblp>"
  //    val test = "<article mdate=\"2017-06-06\" key=\"journals/geb/SonmezU05\">\n<author>Tayfun S&ouml;nmez</author>\n<author orcid=\"0000-0001-7693-1635\">M. Utku &Uuml;nver</author>\n<title>House allocation with existing tenants: an equivalence.</title>\n<pages>153-185</pages>\n<year>2005</year>\n<volume>52</volume>\n<journal>Games and Economic Behavior</journal>\n<number>1</number>\n<ee>https://doi.org/10.1016/j.geb.2004.04.008</ee>\n<url>db/journals/geb/geb52.html#SonmezU05</url>\n</article>"
  //    //    val proceedingsXML = XML.loadString(test)
  //    //    val authors = (proceedingsXML \ "author").map(author => author.text).toList
  //    //    //    print(authors)
  //    //    val csAuthors = authors filter (csProf.contains)
  //    //    println(csAuthors)
  //
  //    val xmlString =
  //      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
  //          <!DOCTYPE dblp SYSTEM "$dtdFilePath">
  //          <dblp>""" + test + "</dblp>"
  //
  ////    val test_xml = start_text + test + end_text
  //    print(xmlString)
  //    XML.loadString(xmlString)
  //    //      val t =XML.load(new java.io.ByteArrayInputStream(test.getBytes(java.nio.charset.StandardCharsets.UTF_16.name)));
  //
  //    val csAuthors = processXML(xmlString)
  //
  //    // Generate all pairs of authors
  //    val authorMappings = generateAuthorMapping(csAuthors)
  //
  //    // Mapper writing all pairs of CS authors working on the processed paper
  //    for (mapping <- authorMappings) println(new Text(mapping._1), new Text(mapping._2))
  //
  //
  //  }

}
