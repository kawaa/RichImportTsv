/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package pl.edu.icm.coansys.richimporttsv.jobs.mapreduce;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import pl.edu.icm.coansys.richimporttsv.algorithm.KMPMatcher;
import pl.edu.icm.coansys.richimporttsv.io.SeparatorInputFormat;
import pl.edu.icm.coansys.richimporttsv.io.SeparatorRecordReader;

/**
 *
 * @author akawa
 */
public class RichImportTsv extends ImportTsv {

    public final static String ROWKEY_COLUMN_SPEC = "HBASE_ROW_KEY";
    public final static String NAME = RichImportTsv.class.getName();
    public final static String INPUT_FORMAT_CONF_KEY = "importtsv.input.format.class";
    public final static String MAPPER_CONF_KEY = "importtsv.mapper.class";
    public final static String SKIP_LINES_CONF_KEY = "importtsv.skip.bad.lines";
    public final static String BULK_OUTPUT_CONF_KEY = "importtsv.bulk.output";
    public final static String COLUMNS_CONF_KEY = "importtsv.columns";
    public final static String SEPARATOR_CONF_KEY = "importtsv.separator";
    public final static String SKIP_UNTIL_SEPARATOR_CONF_KEY = "importtsv.skip.until.separator";
    public final static String RECORD_SEPARATOR_CONF_KEY = "importtsv.record.separator";
    public final static String TIMESTAMP_CONF_KEY = "importtsv.timestamp";
    public final static String DEFAULT_SEPARATOR = "\t";
    public final static String DEFAULT_RECORD_SEPARATOR = "\n";
    public final static Class DEFAULT_MAPPER = RichTsvImporterMapper.class;
    public final static Class DEFAULT_INPUT_FORMAT = SeparatorInputFormat.class;

    static class RichTsvParser {

        /**
         * Column families and qualifiers mapped to the TSV columns
         */
        private final byte[][] families;
        private final byte[][] qualifiers;
        private final byte[] separatorBytes;
        private int rowKeyColumnIndex;
        public static String ROWKEY_COLUMN_SPEC = RichImportTsv.ROWKEY_COLUMN_SPEC;

        /**
         * @param columnsSpecification the list of columns to parser out, comma
         * separated. The row key should be the special token
         * TsvParser.ROWKEY_COLUMN_SPEC
         */
        public RichTsvParser(String columnsSpecification, String separatorStr) {
            // Configure separator
            byte[] separator = Bytes.toBytes(separatorStr);
            separatorBytes = separator;

            // Configure columns
            ArrayList<String> columnStrings = Lists.newArrayList(
                    Splitter.on(',').trimResults().split(columnsSpecification));

            families = new byte[columnStrings.size()][];
            qualifiers = new byte[columnStrings.size()][];

            for (int i = 0; i < columnStrings.size(); i++) {
                String str = columnStrings.get(i);
                if (ROWKEY_COLUMN_SPEC.equals(str)) {
                    rowKeyColumnIndex = i;
                    continue;
                }
                String[] parts = str.split(":", 2);
                if (parts.length == 1) {
                    families[i] = str.getBytes();
                    qualifiers[i] = HConstants.EMPTY_BYTE_ARRAY;
                } else {
                    families[i] = parts[0].getBytes();
                    qualifiers[i] = parts[1].getBytes();
                }
            }
        }

        public int getRowKeyColumnIndex() {
            return rowKeyColumnIndex;
        }

        public byte[] getFamily(int idx) {
            return families[idx];
        }

        public byte[] getQualifier(int idx) {
            return qualifiers[idx];
        }

        public ParsedLine parse(byte[] lineBytes, int length) throws BadTsvLineException {
            // Enumerate separator offsets
            ArrayList<Integer> tabOffsets = new ArrayList<Integer>(families.length);
            int i = 0;
            while (i < length) {
                i = KMPMatcher.indexOf(lineBytes, i, separatorBytes, lineBytes.length);
                if (i == KMPMatcher.FAILURE || i >= length) {
                    break;
                } else {
                    tabOffsets.add(i);
                    i += separatorBytes.length;
                }
            }

            if (tabOffsets.isEmpty()) {
                throw new BadTsvLineException("No delimiter");
            }

            tabOffsets.add(length);

            if (tabOffsets.size() > families.length) {
                throw new BadTsvLineException("Excessive columns");
            } else if (tabOffsets.size() <= getRowKeyColumnIndex()) {
                throw new BadTsvLineException("No row key");
            }
            return new ParsedLine(tabOffsets, lineBytes, separatorBytes.length);
        }

        class ParsedLine {

            private final ArrayList<Integer> tabOffsets;
            private byte[] lineBytes;
            private int separatorSize;

            ParsedLine(ArrayList<Integer> tabOffsets, byte[] lineBytes, int separatorSize) {
                this.tabOffsets = tabOffsets;
                this.lineBytes = lineBytes;
                this.separatorSize = separatorSize;
            }

            public int getRowKeyOffset() {
                return getColumnOffset(rowKeyColumnIndex);
            }

            public int getRowKeyLength() {
                return getColumnLength(rowKeyColumnIndex);
            }

            public int getColumnOffset(int idx) {
                if (idx > 0) {
                    return tabOffsets.get(idx - 1) + separatorSize;
                } else {
                    return 0;
                }
            }

            public int getColumnLength(int idx) {
                return tabOffsets.get(idx) - getColumnOffset(idx);
            }

            public int getColumnCount() {
                return tabOffsets.size();
            }

            public byte[] getLineBytes() {
                return lineBytes;
            }
        }

        public static class BadTsvLineException extends Exception {

            public BadTsvLineException(String err) {
                super(err);
            }
            private static final long serialVersionUID = 1L;
        }
    }

    /**
     * Sets up the actual job.
     *
     * @param conf The current configuration.
     * @param args The command line parameters.
     * @return The newly created job.
     * @throws IOException When setting up the job fails.
     */
    public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException, ClassNotFoundException {

        Job job = ImportTsv.createSubmittableJob(conf, args);

        // See if a non-default InputFormat was set
        String inputFormatClassName = conf.get(INPUT_FORMAT_CONF_KEY);
        Class inputFormatClass = inputFormatClassName != null ? Class.forName(inputFormatClassName) : DEFAULT_INPUT_FORMAT;
        job.setInputFormatClass(inputFormatClass);

        // Setting record separator
        String recordSeparator = conf.get(RECORD_SEPARATOR_CONF_KEY);
        if (recordSeparator == null) {
            recordSeparator = DEFAULT_RECORD_SEPARATOR;
        }
        job.getConfiguration().set(SeparatorRecordReader.RECORD_SEPARATOR_CONF_KEY, recordSeparator);

        // Setting custom mapper, if any
        String mapperClassName = conf.get(MAPPER_CONF_KEY);
        if (mapperClassName == null) {
            job.setMapperClass(DEFAULT_MAPPER);
        }

        return job;
    }

    /*
     * @param errorMsg Error message. Can be null.
     */
    private static void usage(final String errorMsg) {
        if (errorMsg != null && errorMsg.length() > 0) {
            System.err.println("ERROR: " + errorMsg);
        }
        String usage =
                "Usage: " + NAME + " -Dimporttsv.columns=a,b,c <tablename> <inputdir>\n"
                + "\n"
                + "Imports the given input directory of TSV data into the specified table.\n"
                + "\n"
                + "The column names of the TSV data must be specified using the -Dimporttsv.columns\n"
                + "option. This option takes the form of comma-separated column names, where each\n"
                + "column name is either a simple column family, or a columnfamily:qualifier. The special\n"
                + "column name HBASE_ROW_KEY is used to designate that this column should be used\n"
                + "as the row key for each imported record. You must specify exactly one column\n"
                + "to be the row key, and you must specify a column name for every column that exists in the\n"
                + "input data.\n"
                + "\n"
                + "By default importtsv will load data directly into HBase. To instead generate\n"
                + "HFiles of data to prepare for a bulk data load, pass the option:\n"
                + "  -D" + BULK_OUTPUT_CONF_KEY + "=/path/for/output\n"
                + "  Note: if you do not use this option, then the target table must already exist in HBase\n"
                + "\n"
                + "Other options that may be specified with -D include:\n"
                + "  -D" + SKIP_LINES_CONF_KEY + "=false - fail if encountering an invalid line\n"
                + "  -D" + SEPARATOR_CONF_KEY + "=| - eg separate on pipes instead of tabs\n"
                + "  -D" + INPUT_FORMAT_CONF_KEY + "=my.InputFormat - A user-defined InputFormat to use instead of " + DEFAULT_INPUT_FORMAT.getName() + "\n"
                + "  -D" + RECORD_SEPARATOR_CONF_KEY + "=# - eg separate records on # instead of new lines\n"
                + "  -D" + SKIP_UNTIL_SEPARATOR_CONF_KEY + "= - (optional) skip part of the field and put remaining part into HBase cell" + "\n"
                + "  -D" + TIMESTAMP_CONF_KEY + "=currentTimeAsLong - use the specified timestamp for the import\n"
                + "  -D" + MAPPER_CONF_KEY + "=my.Mapper - A user-defined Mapper to use instead of " + DEFAULT_MAPPER.getName() + "\n";

        System.err.println(usage);
    }

    /**
     * Main entry point.
     *
     * @param args The command line parameters.
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {

/*        
         String tableInitName = "enwiki";
         String inputFileName = "/home/akawa/Documents/git-projects/RichImportTsv/src/test/resource/richinput/enwiki.dat";

         args = new String[]{
         "-D" + SEPARATOR_CONF_KEY + "=\n",
         "-D" + RECORD_SEPARATOR_CONF_KEY + "=\n\n",
         "-D" + SKIP_UNTIL_SEPARATOR_CONF_KEY + "= ",
         "-D" + COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,m:cat,m:im,m:main,m:talk,m:us,m:us_talk,m:oth,m:exter,m:templ,m:comm,m:minor,m:textdata",
         tableInitName,
         inputFileName
         };
*/
        main(HBaseConfiguration.create(), args);
    }

    public static void main(Configuration conf, String[] args) throws Exception {
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            usage("Wrong number of arguments: " + otherArgs.length);
            System.exit(-1);
        }

        // Make sure columns are specified
        String columns[] = conf.getStrings(COLUMNS_CONF_KEY);
        if (columns == null) {
            usage("No columns specified. Please specify with -D" + COLUMNS_CONF_KEY + "=...");
            System.exit(-1);
        }

        // Make sure they specify exactly one column as the row key
        int rowkeysFound = 0;
        for (String col : columns) {
            if (col.equals(ROWKEY_COLUMN_SPEC)) {
                rowkeysFound++;
            }
        }
        if (rowkeysFound != 1) {
            usage("Must specify exactly one column as " + ROWKEY_COLUMN_SPEC);
            System.exit(-1);
        }

        // Make sure one or more columns are specified
        if (columns.length < 2) {
            usage("One or more columns in addition to the row key are required");
            System.exit(-1);
        }

        Job job = createSubmittableJob(conf, otherArgs);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
