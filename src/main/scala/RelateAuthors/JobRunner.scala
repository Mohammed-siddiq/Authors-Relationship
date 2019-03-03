package RelateAuthors

import JHelpers.XmlInputFormatWithMultipleTags
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

/**
  * This is the Starting point of the Map reduce Job, defining the configurations of the mapper reducer and the input and output formats
  */
object JobRunner {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val configuration = new Configuration
    val conf = ConfigFactory.load("InputFormat")

    //loading start tags
    configuration.set("xmlinput.start", conf.getString("START_TAGS"))

    //loading end_tags
    configuration.set("xmlinput.end", conf.getString("END_TAGS"))
    configuration.set(
      "io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
    val job = Job.getInstance(configuration, "RelateAuthors")
    println("Entered here...")
    job.setJarByClass(this.getClass)
    //Setting mapper
    job.setMapperClass(classOf[MyMapper])
    job.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
    job.setCombinerClass(classOf[MyReducer])

    //setting reducer
    job.setReducerClass(classOf[MyReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(1)))
    FileOutputFormat.setOutputPath(job, new Path(args(2)))
    logger.debug("Setting up the Job conf....")
    System.exit(if (job.waitForCompletion(true)) 0 else 1)


  }
}