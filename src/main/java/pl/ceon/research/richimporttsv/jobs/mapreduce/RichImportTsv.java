/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pl.ceon.research.richimporttsv.jobs.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import pl.ceon.research.richimporttsv.io.SeparatorInputFormat;
import pl.ceon.research.richimporttsv.io.SeparatorRecordReader;

/**
 *
 * @author akawa
 */
public class RichImportTsv extends ImportTsv {

    public static String ROWKEY_COLUMN_SPEC = "HBASE_ROW_KEY";
    final static String NAME = RichImportTsv.class.getPackage() + "." + RichImportTsv.class.getName();
    final static String INPUT_FORMAT_CONF_KEY = "importtsv.input.format.class";
    final static String MAPPER_CONF_KEY = "importtsv.mapper.class";
    final static String SKIP_LINES_CONF_KEY = "importtsv.skip.bad.lines";
    final static String BULK_OUTPUT_CONF_KEY = "importtsv.bulk.output";
    final static String COLUMNS_CONF_KEY = "importtsv.columns";
    final static String SEPARATOR_CONF_KEY = "importtsv.separator";
    final static String RECORD_SEPARATOR_CONF_KEY = "importtsv.record.separator";
    final static String TIMESTAMP_CONF_KEY = "importtsv.timestamp";
    final static String DEFAULT_SEPARATOR = "\t";
    final static String DEFAULT_RECORD_SEPARATOR = "\n";
    final static Class DEFAULT_MAPPER = TsvImporterMapper.class;
    final static Class DEFAULT_INPUT_FORMAT = SeparatorInputFormat.class;

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
        String recordSeparator = job.getConfiguration().get(RECORD_SEPARATOR_CONF_KEY);
        if (recordSeparator == null) {
            recordSeparator = DEFAULT_RECORD_SEPARATOR;
        }

        job.getConfiguration().set(SeparatorRecordReader.RECORD_SEPARATOR_CONF_KEY, recordSeparator);
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
