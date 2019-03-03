package RelateAuthors

import java.lang

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * Reducer implementation that will just add the papers published by professors which is emitted by our Mapper.
  * Writing the total number of papers associated with a professor/professors
  */

class MyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {


  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    val sum = values.asScala.foldLeft(0)(_ + _.get)
//    print("Reducer writing : ", key, new IntWritable(sum))
    logger.info("writing Key: ", key)
//    logger.debug("writing Key: ", key)
    context.write(key, new IntWritable(sum))
  }
}






