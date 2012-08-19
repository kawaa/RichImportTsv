/**
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRichImportTsv {

    private static final Log LOG = LogFactory.getLog(TestRichImportTsv.class);
    private static HBaseTestingUtility UTIL;
    // some prefefinied names
    final protected long TEST_ROW_COUNT = 100;
    final protected String S_ROW_PREFIX = "row";
    final protected String S_COLUMN_FAMILY = "cf";
    final protected String S_COLUMN_QUALIFIER = "cq";
    final protected String S_COLUMN_QUALIFIER2 = "cq2";
    final protected byte[] B_COLUMN_FAMILY = Bytes.toBytes(S_COLUMN_FAMILY);
    final protected byte[] B_COLUMN_QUALIFIER = Bytes.toBytes(S_COLUMN_QUALIFIER);
    final protected byte[] B_COLUMN_QUALIFIER2 = Bytes.toBytes(S_COLUMN_QUALIFIER2);
    final protected byte[] B_VALUE = Bytes.toBytes("value");

    private String getCurrentDateAppended(String name) {
        return name + "-" + new Date().getTime();
    }

    private void dropTable(String tableName) {
        try {
            UTIL.deleteTable(Bytes.toBytes(tableName));
        } catch (IOException ex) {
            LOG.info("Table can not be deleted: " + tableName + "\n" + ex.getLocalizedMessage());
        }
    }

    private HTable createAndPopulateDefaultTable(String tableName, long rowCount) throws IOException, InterruptedException {
        HTable htable = UTIL.createTable(Bytes.toBytes(tableName), B_COLUMN_FAMILY);
        List<Row> putList = new ArrayList<Row>();
        for (long i = 0; i < rowCount; ++i) {
            Put put = new Put(Bytes.toBytes(S_ROW_PREFIX + i));
            put.add(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER, B_VALUE);
            putList.add(put);
        }
        htable.batch(putList);
        return htable;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {

       
        Configuration conf = new Configuration();
//        File workingDirectory = new File("./");
//        System.setProperty("test.build.data", workingDirectory.getAbsolutePath());
//        conf.set("test.build.data", new File(workingDirectory, "zookeeper").getAbsolutePath());
//        conf.set("fs.default.name", "file:///");
//        conf.set("zookeeper.session.timeout", "180000");
        conf.set("hbase.zookeeper.peerport", "2889");
        conf.set("hbase.zookeeper.property.clientPort", "2182");
        conf.set("hbase.master.port", "6001");
        conf.set("hbase.master.info.port", "6011");
        conf.set("hbase.regionserver.port", "6021");
        conf.set("hbase.regionserver.info.port", "6031");

        UTIL = new HBaseTestingUtility(conf);
        UTIL.startMiniCluster();
        UTIL.startMiniMapReduceCluster();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UTIL.shutdownMiniMapReduceCluster();
        UTIL.shutdownMiniCluster();
    }

    @Test(timeout = 1800000)
    public void testTableRichImportTsv() throws Exception {

        String tableInitName = getCurrentDateAppended("testTableRichImportTsv");
        String inputFileName = "InputFile.dat";

        String[] args = new String[]{
            "-Dimporttsv.record.separator=#",
            "-Dimporttsv.separator=$",
            "-Dimporttsv.columns=HBASE_ROW_KEY," + S_COLUMN_FAMILY + ":" + S_COLUMN_QUALIFIER,
            tableInitName,
            inputFileName
        };

        HTable htableImport = doMROnTableTest(inputFileName, S_COLUMN_FAMILY, tableInitName, "KEY1$VALUE\n1#KEY2$VALUE2#\nKEY3$VALUE3", args);

        Result key1 = htableImport.get(new Get(Bytes.toBytes("KEY1")));
        assertNotNull(key1);
        assertEquals("VALUE\n1", Bytes.toString(key1.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));
        Result key3 = htableImport.get(new Get(Bytes.toBytes("\nKEY3")));
        assertNotNull(key3);
        assertEquals("VALUE3", Bytes.toString(key3.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));

        dropTable(tableInitName);
    }

    @Test(timeout = 1800000)
    public void testMultiCharacterSeparatorsTableRichImportTsv() throws Exception {

        String tableInitName = getCurrentDateAppended("testTableRichImportTsv");
        String inputFileName = "InputFile.dat";

        String[] args = new String[]{
            "-Dimporttsv.record.separator=###",
            "-Dimporttsv.separator=$$$",
            "-Dimporttsv.columns=HBASE_ROW_KEY," + S_COLUMN_FAMILY + ":" + S_COLUMN_QUALIFIER,
            tableInitName,
            inputFileName
        };

        HTable htableImport = doMROnTableTest(inputFileName, S_COLUMN_FAMILY, tableInitName,
                "KEY1$$$VALUE\n1###KEY2$$$VALUE2###\nKEY3$$$VALUE3", args);

        Result key1 = htableImport.get(new Get(Bytes.toBytes("KEY1")));
        assertNotNull(key1);
        assertEquals("VALUE\n1", Bytes.toString(key1.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));
        Result key3 = htableImport.get(new Get(Bytes.toBytes("\nKEY3")));
        assertNotNull(key3);
        assertEquals("VALUE3", Bytes.toString(key3.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));

        dropTable(tableInitName);
    }

    @Test(timeout = 1800000)
    public void testMultiCharacterSeparatorsMultiColumnInputTableRichImportTsv() throws Exception {

        String tableInitName = getCurrentDateAppended("testTableRichImportTsv");
        String inputFileName = "InputFile.dat";

        String[] args = new String[]{
            "-Dimporttsv.record.separator=###",
            "-Dimporttsv.separator=$$$",
            "-Dimporttsv.columns=HBASE_ROW_KEY," 
                + S_COLUMN_FAMILY + ":" + S_COLUMN_QUALIFIER + "," 
                + S_COLUMN_FAMILY + ":" + S_COLUMN_QUALIFIER2,
            tableInitName,
            inputFileName
        };

        HTable htableImport = doMROnTableTest(inputFileName, S_COLUMN_FAMILY, tableInitName,
                "KEY1$$$VALUEa$$$VALUEb\n###KEY2$$$VALUE2$$$VALUE2b###\nKEY3$$$VALUE3$$$VALUE3b", args);

        Result key1 = htableImport.get(new Get(Bytes.toBytes("KEY1")));
        assertNotNull(key1);
        assertEquals("VALUEa", Bytes.toString(key1.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));
        assertEquals("VALUEb\n", Bytes.toString(key1.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER2)));

        Result key2 = htableImport.get(new Get(Bytes.toBytes("KEY2")));
        assertNotNull(key2);
        assertEquals("VALUE2", Bytes.toString(key2.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));
        assertEquals("VALUE2b", Bytes.toString(key2.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER2)));

        Result key3 = htableImport.get(new Get(Bytes.toBytes("\nKEY3")));
        assertNotNull(key3);
        assertEquals("VALUE3", Bytes.toString(key3.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));
        assertEquals("VALUE3b", Bytes.toString(key3.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER2)));

        dropTable(tableInitName);
    }

    @Test(timeout = 1800000)
    public void testTextInputFormatTableRichImportTsv() throws Exception {

        String tableInitName = getCurrentDateAppended("testTableRichImportTsv");
        String inputFileName = "InputFile.dat";

        String[] args = new String[]{
            "-Dimporttsv.input.format.class=org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
            "-Dimporttsv.separator=$",
            "-Dimporttsv.columns=HBASE_ROW_KEY," + S_COLUMN_FAMILY + ":" + S_COLUMN_QUALIFIER,
            tableInitName,
            inputFileName
        };

        HTable htableImport = doMROnTableTest(inputFileName, S_COLUMN_FAMILY, tableInitName, "KEY1$VALUE1\nKEY2$VALUE2\nKEY3$VALUE3", args);

        Result key1 = htableImport.get(new Get(Bytes.toBytes("KEY1")));
        assertNotNull(key1);
        assertEquals("VALUE1", Bytes.toString(key1.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));
        Result key3 = htableImport.get(new Get(Bytes.toBytes("KEY3")));
        assertNotNull(key3);
        assertEquals("VALUE3", Bytes.toString(key3.getValue(B_COLUMN_FAMILY, B_COLUMN_QUALIFIER)));

        dropTable(tableInitName);
    }

    @Test
    public void testDirRichImportTsv() throws Exception {
        String tableInitName = getCurrentDateAppended("testDirRichImportTsv");
        String inputFileName = "InputFile.dat";
        String outputDirName = getCurrentDateAppended("richtsv-output");
        FileSystem dfs = UTIL.getDFSCluster().getFileSystem();

        Path qualifiedOutputDir = dfs.makeQualified(new Path(outputDirName));
        assertFalse(dfs.exists(qualifiedOutputDir));

        // Prepare the arguments required for the test.
        String[] args = new String[]{
            "-Dimporttsv.record.separator=#",
            "-Dimporttsv.separator=$",
            "-Dimporttsv.columns=HBASE_ROW_KEY," + S_COLUMN_FAMILY + ":" + S_COLUMN_QUALIFIER,
            "-Dimporttsv.bulk.output=" + outputDirName,
            tableInitName,
            inputFileName
        };

        doMROnTableTest(inputFileName, S_COLUMN_FAMILY, tableInitName, "KEY1$VALUE\n1#KEY2$VALUE2#\nKEY3$VALUE3", args);

        assertTrue(dfs.exists(qualifiedOutputDir));
    }

    private HTable doMROnTableTest(String inputFile, String family, String tableName, String line, String[] args) throws Exception {

        GenericOptionsParser opts = new GenericOptionsParser(UTIL.getConfiguration(), args);
        Configuration config = UTIL.getConfiguration();
        args = opts.getRemainingArgs();

        FileSystem fs = UTIL.getDFSCluster().getFileSystem();
        FSDataOutputStream op = fs.create(new Path(inputFile), true);
        op.write(line.getBytes(HConstants.UTF8_ENCODING));
        op.close();

        assertTrue(fs.exists(new Path(inputFile)));

        final byte[] FAM = Bytes.toBytes(family);
        final byte[] TAB = Bytes.toBytes(tableName);

        HTable htableImport = UTIL.createTable(TAB, FAM);
        assertEquals(0, UTIL.countRows(htableImport));

        Job job = RichImportTsv.createSubmittableJob(config, args);
        job.waitForCompletion(false);
        assertTrue(job.isSuccessful());
        return htableImport;
    }

    //@Test(timeout = 1800000)
    public void testRowCounter() throws Exception {
        String tableInitName = getCurrentDateAppended("testRowCounter");
        createAndPopulateDefaultTable(tableInitName, TEST_ROW_COUNT);

        Job job = RowCounter.createSubmittableJob(UTIL.getConfiguration(), new String[]{tableInitName});
        job.waitForCompletion(true);
        long count = job.getCounters().findCounter("org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters", "ROWS").getValue();
        Assert.assertEquals(TEST_ROW_COUNT, count);

        dropTable(tableInitName);
    }

    //@Test(timeout = 1800000)
    public void testCopy() throws Exception {

        String tableInitName = getCurrentDateAppended("testCopy");
        createAndPopulateDefaultTable(tableInitName, TEST_ROW_COUNT);

        final String tableCopyName = tableInitName + "Copy";
        HTable htableCopy = UTIL.createTable(Bytes.toBytes(tableCopyName), B_COLUMN_FAMILY);

        Job job = CopyTable.createSubmittableJob(UTIL.getConfiguration(), new String[]{"--new.name=" + tableCopyName, tableInitName});
        job.waitForCompletion(true);
        Assert.assertEquals(TEST_ROW_COUNT, (long) UTIL.countRows(htableCopy));

        dropTable(tableInitName);
        dropTable(tableCopyName);
    }

    //@Test(timeout = 1800000)
    public void testExportImport() throws Exception {

        String tableInitName = getCurrentDateAppended("testExportImport");
        createAndPopulateDefaultTable(tableInitName, TEST_ROW_COUNT);

        FileSystem dfs = UTIL.getDFSCluster().getFileSystem();
        Path qualifiedTempDir = dfs.makeQualified(new Path("export-import-temp-dir"));
        Assert.assertFalse(dfs.exists(qualifiedTempDir));

        Job jobExport = Export.createSubmittableJob(UTIL.getConfiguration(), new String[]{tableInitName, qualifiedTempDir.toString()});
        jobExport.waitForCompletion(true);

        Assert.assertTrue(dfs.exists(qualifiedTempDir));

        final String tableImportName = tableInitName + "Import";
        HTable htableImport = UTIL.createTable(Bytes.toBytes(tableImportName), B_COLUMN_FAMILY);

        Job jobImport = Import.createSubmittableJob(UTIL.getConfiguration(), new String[]{tableImportName, qualifiedTempDir.toString()});
        jobImport.waitForCompletion(true);
        Assert.assertEquals(TEST_ROW_COUNT, (long) UTIL.countRows(htableImport));

        dropTable(tableInitName);
        dropTable(tableImportName);
    }
}
