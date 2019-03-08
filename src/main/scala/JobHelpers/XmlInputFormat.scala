package JHelpers

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import java.io.IOException
import java.nio.charset.StandardCharsets


/**
  * Standard implementation of the XMLInputFormat that reads a single start and end tag
  */
object XmlInputFormat {
  val START_TAG_KEY = "xmlinput.start"
  val END_TAG_KEY = "xmlinput.end"

  class XmlRecordReader extends RecordReader[LongWritable, Text] {
    private var startTag = null
    private var endTag = null
    private var start = 0L
    private var end = 0L
    private var fsin = null
    private val buffer = new DataOutputBuffer
    private val key = new LongWritable
    private val value = new Text

    @throws[IOException]
    @throws[InterruptedException]
    override def initialize(is: InputSplit, tac: TaskAttemptContext): Unit = {
      val fileSplit = is.asInstanceOf[FileSplit]
      startTag = tac.getConfiguration.get(START_TAG_KEY).getBytes(StandardCharsets.UTF_8)
      endTag = tac.getConfiguration.get(END_TAG_KEY).getBytes(StandardCharsets.UTF_8)
      start = fileSplit.getStart
      end = start + fileSplit.getLength
      val file = fileSplit.getPath
      val fs = file.getFileSystem(tac.getConfiguration)
      fs.open(file)
      fsin = fs.open(fileSplit.getPath)
      fsin.seek(start)
    }

    @throws[IOException]
    @throws[InterruptedException]
    override def nextKeyValue: Boolean = {
      if (fsin.getPos < end) if (readUntilMatch(startTag, false)) { // Read until start_tag1 or start_tag2
        try {
          buffer.write(startTag)
          if (readUntilMatch(endTag, true)) { // changed here
            value.set(buffer.getData, 0, buffer.getLength)
            key.set(fsin.getPos)
            return true
          }
        } finally buffer.reset
      }
      false
    }

    @throws[IOException]
    @throws[InterruptedException]
    override def getCurrentKey: LongWritable = key

    @throws[IOException]
    @throws[InterruptedException]
    override def getCurrentValue: Text = value

    @throws[IOException]
    @throws[InterruptedException]
    override def getProgress: Float = (fsin.getPos - start) / (end - start).toFloat

    @throws[IOException]
    override def close(): Unit = {
      fsin.close()
    }

    @throws[IOException]
    private def readUntilMatch(`match`: Array[Byte], withinBlock: Boolean): Boolean = {
      var i = 0
      while ( {
        true
      }) {
        val b = fsin.read
        // end of file:
        if (b == -1) return false
        // save to buffer:
        if (withinBlock) buffer.write(b)
        // check if we're matching:
        if (b == `match`(i)) {
          i += 1
          if (i >= `match`.length) return true
        }
        else i = 0
        // see if we've passed the stop point:
        if (!withinBlock && i == 0 && fsin.getPos >= end) return false
      }
    }
  }

}

class XmlInputFormat extends TextInputFormat {
  override def createRecordReader(is: InputSplit, tac: TaskAttemptContext) = new XmlInputFormat.XmlRecordReader
}
