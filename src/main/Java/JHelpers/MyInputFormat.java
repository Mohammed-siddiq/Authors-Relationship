package JHelpers;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MyInputFormat extends TextInputFormat {

    public static final String START_TAG_KEYS = "xmlinput.start";
    public static final String END_TAG_KEYS = "xmlinput.end";

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) {


        return new XmlRecordReader();


    }

    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private byte[][] startTags;
        private byte[][] endTags;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();

        static Logger logger = LoggerFactory.getLogger("XmlRecordReader");

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) is;
            String[] sTags = tac.getConfiguration().get(START_TAG_KEYS).split(",");
            for (String s :
                    sTags) {
                System.out.println("Start tag : " + s);
                logger.info("Start tag : " + s);
                logger.debug("Start tag : " + s);

            }
            String[] eTags = tac.getConfiguration().get(END_TAG_KEYS).split(",");
            startTags = new byte[sTags.length][];
            endTags = new byte[sTags.length][];
            for (int i = 0; i < sTags.length; i++) {
                startTags[i] = sTags[i].getBytes(StandardCharsets.UTF_8);
                endTags[i] = eTags[i].getBytes(StandardCharsets.UTF_8);

            }
//            startTags = tac.getConfiguration().get(START_TAG_KEYS).getBytes(StandardCharsets.UTF_8);
//            endTags = tac.getConfiguration().get(END_TAG_KEYS).getBytes(StandardCharsets.UTF_8);


            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();

            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);


        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                int res = readUntilMatch(startTags, false, 0);
                if (res != -1) {
                    try {
                        buffer.write(startTags[res]);
                        logger.info("found match tag :", res);
                        int res1 = readUntilMatch(endTags, true, res);
                        if (res1 != -1) {

                            value.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;


        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        private int readUntilMatch(byte[][] match, boolean withinBlock, int whichTag) throws IOException {
            int i = 0;
            long currentPosition = fsin.getPos();


            // match for starting tag
            while (!withinBlock) {
                for (int j = 0; !withinBlock && j < match.length; j++, i = 0) {
                    String t = new String(match[j]);
                    fsin.seek(currentPosition);
                    System.out.println("Checking for Start tag : " + t);
                    logger.info("Iterator " + j + "Start tag : " + t);
                    logger.debug("Iterator" + j + "Start tag : " + t);


                    int b = fsin.read();
                    // end of file:
                    if (b == -1) {
                        logger.info("Iterator : " + j + "returning -1");
                        return -1;
                    }

                    logger.info("Iterator " + j + "Values : b " + b + " - text" + match[j][i]);


                    // check if we're matching:
                    if (b == match[j][i]) { // or check for match with start_tag 2
                        logger.info("Iterator : " + j + "MAtched ? " + b + " with " + match[j][i]);
                        i++;
                        if (i >= match[j].length) {
                            logger.info("Iterator : " + j + "returning");
                            return j;
                        }
                    } else i = 0;


                    // see if we've passed the stop point:
                    if (i == 0 && fsin.getPos() >= end) continue;

                    // we have found the tag
                    if (i != 0) {
                        logger.info("Iterator : " + j + " Inside for Ith value " + i);
                        while (true) {
                            int cb = fsin.read();
                            // end of file:
                            if (cb == -1) return -1;

                            // check if we're matching:
                            if (cb == match[j][i]) {
                                logger.info("Iterator : " + j + " Matched  " + cb + " with " + match[j][i]);
                                i++;
                                if (i >= match[j].length) {
                                    logger.info("Iterator : " + j + " returning " + new String(match[j]) + " with i's value  " + i);
                                    return j;
                                }
                            } else i = 0;
                            // see if we've passed the stop point:
                            if (i == 0 && fsin.getPos() >= end) return -1;

                            if (i == 0) {
                                logger.info("Breaking to continue with the next tag!!! ");
                                break;
                            }

                        }
                    }
                    logger.info("Iterator : " + j + "Should proceed to next tag ");
                }
                if ((fsin.getPos() + 5) >= end) return -1;
                else currentPosition += 5;
            }


            //extracting data after tag is matched
            if (withinBlock) {
                logger.info("Should never be here " + whichTag);
                while (true) {
                    int b = fsin.read();
                    // end of file:
                    if (b == -1) return -1;
                    // save to buffer:
                    if (withinBlock) buffer.write(b);

                    // check if we're matching:
                    if (b == match[whichTag][i]) { // or check for match with start_tag 2
                        i++;
                        if (i >= match[whichTag].length) return 1;
                    } else i = 0;
                }

            }
            logger.info("Returning -1 from last");
            return -1;
        }


    }


}
