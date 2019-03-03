import RelateAuthors.MyMapper
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec


/**
  * Verifies the important units of Mapper
  */

class VerifyMapper extends FlatSpec {

  val authorMapper = new MyMapper
  val configuration = new Configuration
  val conf = ConfigFactory.load("InputFormat")

  "ProcessXML Method" should "  process the passed XML and return only the cs authors" in {
    val testXML = "    <inproceedings mdate=\"2017-05-24\" key=\"conf/icst/GrechanikHB13\">\n<author>Ugo Buy</author>\n<author>B. M. Mainul Hossain</author>\n<author>Mark Grechanik</author>\n<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>\n<pages>174-183</pages>\n<year>2013</year>\n<booktitle>ICST</booktitle>\n<ee>https://doi.org/10.1109/ICST.2013.19</ee>\n<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>\n<crossref>conf/icst/2013</crossref>\n<url>db/conf/icst/icst2013.html#GrechanikHB13</url>\n</inproceedings>"
    val response = authorMapper.processXML(testXML)

    assert(response.length == 2, "Did not return the CS authors")
    assert(response(0) == "Mark Grechanik".toLowerCase)
    assert(response(1) == "Ugo Buy".toLowerCase)


  }


  "Generate author Mappings" should "Give the combination of authors" in {
    val testAuthors = List("author1", "author2", "author3", "author4")
    val mappings = authorMapper.generateAuthorMapping(testAuthors)
    assert(mappings.length == 6)
  }


}
