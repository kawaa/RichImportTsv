/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.edu.icm.coansys.richimporttsv.jobs.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.hadoop.hbase.mapreduce.TsvImporterMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Write table content out to files in hdfs.
 */
public class RichTsvImporterMapper extends TsvImporterMapper {

    private RichImportTsv.RichTsvParser parser;
    // Timestamp for all inserted rows
    private long ts;
    // Column seperator
    private String separator;
    // Should skip bad lines
    private boolean skipBadLines;
    private Counter badLineCount;

    @Override
    public long getTs() {
        return ts;
    }

    @Override
    public boolean getSkipBadLines() {
        return skipBadLines;
    }

    @Override
    public Counter getBadLineCount() {
        return badLineCount;
    }

    @Override
    public void incrementBadLineCount(int count) {
        badLineCount.increment(count);
    }

    @Override
    protected void setup(Context context) {
        doSetup(context);
        Configuration conf = context.getConfiguration();
        parser = new RichImportTsv.RichTsvParser(conf.get(RichImportTsv.COLUMNS_CONF_KEY), separator);
        if (parser.getRowKeyColumnIndex() == -1) {
            throw new RuntimeException("No row key column specified");
        }
    }

    /**
     * Handles common parameter initialization that a subclass might want to
     * leverage.
     *
     * @param context
     */
    @Override
    protected void doSetup(Context context) {
        Configuration conf = context.getConfiguration();

        // If a custom separator has been used, decode it back from Base64 encoding.
        separator = conf.get(RichImportTsv.SEPARATOR_CONF_KEY);
        separator = (separator == null ? RichImportTsv.DEFAULT_SEPARATOR : new String(Base64.decode(separator)));

        ts = conf.getLong(RichImportTsv.TIMESTAMP_CONF_KEY, System.currentTimeMillis());

        skipBadLines = context.getConfiguration().getBoolean(RichImportTsv.SKIP_LINES_CONF_KEY, true);
        badLineCount = context.getCounter("RichImportTsv", "Bad Lines");
    }

    private void handleBadLines(long offset, RichImportTsv.RichTsvParser.BadTsvLineException badLine) {
        System.err.println("Bad line at offset: " + offset + ":\n" + (badLine != null ? badLine.getMessage() : ""));
        incrementBadLineCount(1);
    }

    /**
     * Convert a line of TSV text into an HBase table row.
     */
    @Override
    public void map(LongWritable offset, Text value, Context context) throws IOException {
        byte[] lineBytes = value.getBytes();

        try {
            RichImportTsv.RichTsvParser.ParsedLine parsed = parser.parse(lineBytes, value.getLength());
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength());

            Put put = new Put(rowKey.copyBytes());
            for (int i = 0; i < parsed.getColumnCount(); i++) {
                if (i == parser.getRowKeyColumnIndex()) {
                    continue;
                }
                KeyValue kv = new KeyValue(
                        lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
                        parser.getFamily(i), 0, parser.getFamily(i).length,
                        parser.getQualifier(i), 0, parser.getQualifier(i).length,
                        ts,
                        KeyValue.Type.Put,
                        lineBytes, parsed.getColumnOffset(i), parsed.getColumnLength(i));
                put.add(kv);
            }
            context.write(rowKey, put);
        } catch (RichImportTsv.RichTsvParser.BadTsvLineException badLine) {
            if (skipBadLines) {
                handleBadLines(offset.get(), badLine);
            } else {
                throw new IOException(badLine);
            }
        } catch (IllegalArgumentException e) {
            if (skipBadLines) {
                handleBadLines(offset.get(), null);
            } else {
                throw new IOException(e);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
