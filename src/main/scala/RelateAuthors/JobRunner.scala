package RelateAuthors

import JHelpers.MyInputFormat
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object JobRunner {

  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val configuration = new Configuration
    val conf = ConfigFactory.load("InputFormat")
    configuration.set("xmlinput.start", conf.getString("START_TAGS"))
    configuration.set("xmlinput.end", conf.getString("END_TAGS"))
    configuration.set(
      "io.serializations",
      "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
    val job = Job.getInstance(configuration, "RelateAuthors")
    println("Entered here...")
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[MyMapper])
    job.setInputFormatClass(classOf[MyInputFormat])
    job.setCombinerClass(classOf[MyReducer])
    job.setReducerClass(classOf[MyReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    print(args(0))
    print(args(1))
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    logger.debug("Setting up the Job conf....")
    System.exit(if (job.waitForCompletion(true)) 0 else 1)


  }
}