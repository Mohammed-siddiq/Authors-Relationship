import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec


/**
  * Verifying the configs loading and usage which would be used by the mappers and the job runner
  */

class VerifyConfigLoading extends FlatSpec {
  val configuration = new Configuration
  val conf = ConfigFactory.load("InputFormat")

  "Load start Tags and End_Tags" should "Return the the list of start tags" in {
    val result = conf.getString("START_TAGS")
    assert(result.split(",").length == 7)
    val result1 = conf.getString("END_TAGS")
    assert(result1.split(",").length == 7)
  }

  "Conf loader" should "Load the confs from the conf" in {

    val result = conf.getStringList("UIC_CS_PROFESSORS")
    assert(result.size() == 56)
  }

  "Load DTD" should "Load the dtd's path as the URI" in {
    val dtdFilePath = getClass.getClassLoader.getResource("dblp.dtd").toURI
    assert(dtdFilePath.toString.length > 0)

  }


}
