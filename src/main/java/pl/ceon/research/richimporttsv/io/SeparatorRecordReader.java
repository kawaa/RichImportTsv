package pl.ceon.research.richimporttsv.io;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes;

public class SeparatorRecordReader extends RecordReader<LongWritable, Text> {

    final public static String RECORD_SEPARATOR_CONF_KEY = "record.separator";
    final static String DEFAULT_SEPARATOR = "\n";
    private long start;
    private long end;
    private long bytesConsumed = 0;
    private long bytesToConsume;
    private boolean stillInChunk = true;
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private FSDataInputStream fsin;
    private DataOutputBuffer buffer = new DataOutputBuffer();
    private byte[] endTag;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        endTag = Bytes.toBytes(conf.get(RECORD_SEPARATOR_CONF_KEY, DEFAULT_SEPARATOR));
                
        FileSplit split = (FileSplit) inputSplit;
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);

        fsin = fs.open(path);
        start = split.getStart();
        end = split.getStart() + split.getLength();
        bytesToConsume = end - start;
        fsin.seek(start);

        if (start != 0) {
            // we are probably starting in the middle of a record
            // so read this one and discard, as the previous call
            // on the preceding chunk read this one already
            readUntilMatch(endTag, false);
        }
    }

    public boolean nextKeyValue() throws IOException {
        if (!stillInChunk) {
            return false;
        }

        // status is true as long as we're still within the
        // chunk we got (i.e., fsin.getPos() < end). If we've
        // read beyond the chunk it will be false
        boolean status = readUntilMatch(endTag, true);
        int bufferContentLength = buffer.getLength() - (status ? endTag.length : 0);
        value = new Text();
        
        value.set(buffer.getData(), 0, bufferContentLength);
        key = new LongWritable(fsin.getPos());
        bytesConsumed += buffer.getLength();
        buffer.reset();

        if (!status) {
            stillInChunk = false;
        }

        return true;
    }

    public float getProgress() throws IOException, InterruptedException {
        return (bytesToConsume == 0 ? 0f : bytesConsumed / bytesToConsume);
    }

    public void close() throws IOException {
        fsin.close();
    }

    private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
        int i = 0;
        while (true) {
            int b = fsin.read();
            if (b == -1) {
                return false;
            }
            if (withinBlock) {
                buffer.write(b);
            }
            if (b == match[i]) {
                i++;
                if (i >= match.length) {
                    return fsin.getPos() < end;
                }
            } else {
                i = 0;
            }
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
       return value;
    }
}