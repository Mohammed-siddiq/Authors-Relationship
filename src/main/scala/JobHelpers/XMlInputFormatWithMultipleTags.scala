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
  * An implementation of XML input format. fine tuned to support multiple start and end tags sent from the configuration
  */


class XmlInputFormatWithMultipleTags extends TextInputFormat {
  override def createRecordReader(is: InputSplit, tac: TaskAttemptContext) = new XmlInputFormatWithMultipleTags.XmlRecordReader
}

object XmlInputFormatWithMultipleTags {
  val START_TAG_KEYS = "xmlinput.start"
  val END_TAG_KEYS = "xmlinput.end"

  class XmlRecordReader extends RecordReader[LongWritable, Text] {
    private var startTags = null
    private var endTags = null
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
      val sTags = tac.getConfiguration.get(START_TAG_KEYS).split(",")
      val eTags = tac.getConfiguration.get(END_TAG_KEYS).split(",")
      startTags = new Array[Array[Byte]](sTags.length)
      endTags = new Array[Array[Byte]](sTags.length)
      var i = 0
      while ( {
        i < sTags.length
      }) {
        startTags(i) = sTags(i).getBytes(StandardCharsets.UTF_8)
        endTags(i) = eTags(i).getBytes(StandardCharsets.UTF_8)

        {
          i += 1;
          i - 1
        }
      }
      start = fileSplit.getStart
      end = start + fileSplit.getLength
      val file = fileSplit.getPath
      val fs = file.getFileSystem(tac.getConfiguration)
      fsin = fs.open(fileSplit.getPath)
      fsin.seek(start)
    }

    @throws[IOException]
    @throws[InterruptedException]
    override def nextKeyValue: Boolean = {
      if (fsin.getPos < end) { //Changed here to perform readuntillmatch to all the tags
        val res = readUntilMatch(startTags, false)
        if (res != -1) { // Read until start_tag1 or start_tag2
          try {
            buffer.write(startTags(res - 1))
            //Changed to read all the contents before the end tag
            val res1 = readUntilMatch(endTags, true)
            if (res1 != -1) { // changed here
              // updating the buffer with contents between start and end tags.
              value.set(buffer.getData, 0, buffer.getLength)
              key.set(fsin.getPos)
              return true
            }
          } finally buffer.reset
        }
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
    private def readUntilMatch(`match`: Array[Array[Byte]], withinBlock: Boolean): Int = {
      var i1 = 0
      var i2 = 0
      var i3 = 0
      var i4 = 0
      var i5 = 0
      var i6 = 0
      var i7 = 0
      while ( {
        true
      }) {
        val b = fsin.read
        // end of file:
        if (b == -1) return -1
        // save to buffer:
        if (withinBlock) buffer.write(b)
        // check if we're matching:
        if (b == `match`(0)(i1)) {
          i1 += 1
          if (i1 >= `match`(0).length) return 1
        }
        else i1 = 0
        if (b == `match`(1)(i2)) {
          i2 += 1
          if (i2 >= `match`(1).length) return 2
        }
        else i2 = 0
        if (b == `match`(2)(i3)) {
          i3 += 1
          if (i3 >= `match`(2).length) return 3
        }
        else i3 = 0
        if (b == `match`(3)(i4)) {
          i4 += 1
          if (i4 >= `match`(3).length) return 4
        }
        else i4 = 0
        if (b == `match`(4)(i5)) {
          i5 += 1
          if (i5 >= `match`(4).length) return 5
        }
        else i5 = 0
        if (b == `match`(5)(i6)) {
          i6 += 1
          if (i6 >= `match`(5).length) return 6
        }
        else i6 = 0
        if (b == `match`(6)(i7)) {
          i7 += 1
          if (i7 >= `match`(6).length) return 7
        }
        else i7 = 0
        // see if we've passed the stop point:
        if (!withinBlock && (i1 == 0 && i2 == 0 && i3 == 0 && i4 == 0 && i5 == 0 && i6 == 0 && i7 == 0) && fsin.getPos >= end) return -1
      }
    }
  }

}

