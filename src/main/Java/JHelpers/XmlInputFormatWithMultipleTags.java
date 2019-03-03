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

import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * An implementation of XML input format. fine tuned to support multiple start and end tags sent from the configuration
 */
public class XmlInputFormatWithMultipleTags extends TextInputFormat {

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


        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) is;

            String[] sTags = tac.getConfiguration().get(START_TAG_KEYS).split(",");

            String[] eTags = tac.getConfiguration().get(END_TAG_KEYS).split(",");
            startTags = new byte[sTags.length][];
            endTags = new byte[sTags.length][];
            for (int i = 0; i < sTags.length; i++) {
                startTags[i] = sTags[i].getBytes(StandardCharsets.UTF_8);
                endTags[i] = eTags[i].getBytes(StandardCharsets.UTF_8);

            }
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
                //Changed here to perform readuntillmatch to all the tags
                int res = readUntilMatch(startTags, false);
                if (res != -1) { // Read until start_tag1 or start_tag2
                    try {

                        buffer.write(startTags[res - 1]);
                        //Changed to read all the contents before the end tag
                        int res1 = readUntilMatch(endTags, true);
                        if (res1 != -1) { // changed here
                            // updating the buffer with contents between start and end tags.
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

        private int readUntilMatch(byte[][] match, boolean withinBlock) throws IOException {
            int i1 = 0, i2 = 0, i3 = 0, i4 = 0, i5 = 0, i6 = 0, i7 = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1) return -1;
                // save to buffer:
                if (withinBlock) buffer.write(b);

                // check if we're matching:
                if (b == match[0][i1]) {
                    i1++;
                    if (i1 >= match[0].length) return 1;
                } else i1 = 0;

                if (b == match[1][i2]) {
                    i2++;
                    if (i2 >= match[1].length) return 2;
                } else i2 = 0;

                if (b == match[2][i3]) {
                    i3++;
                    if (i3 >= match[2].length) return 3;
                } else i3 = 0;

                if (b == match[3][i4]) {
                    i4++;
                    if (i4 >= match[3].length) return 4;
                } else i4 = 0;

                if (b == match[4][i5]) {
                    i5++;
                    if (i5 >= match[4].length) return 5;
                } else i5 = 0;

                if (b == match[5][i6]) {
                    i6++;
                    if (i6 >= match[5].length) return 6;
                } else i6 = 0;

                if (b == match[6][i7]) {
                    i7++;
                    if (i7 >= match[6].length) return 7;
                } else i7 = 0;


                // see if we've passed the stop point:
                if (!withinBlock && (i1 == 0 && i2 == 0 && i3 == 0 && i4 == 0 && i5 == 0 && i6 == 0 && i7 == 0) && fsin.getPos() >= end)
                    return -1;
            }
        }

    }


}
