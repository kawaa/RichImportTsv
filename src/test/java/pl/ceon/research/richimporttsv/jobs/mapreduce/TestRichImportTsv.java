/**
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



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

public class TestRichImportTsv extends GenericTestCase{

    private Log LOG = LogFactory.getLog(TestRichImportTsv.class);
    
    //@Test
    public void testTableRichImportTsv() throws Exception {
 
        String tableInitName = getCurrentDateAppended("testTableRichImportTsv");
        String inputFileName = "InputFile.dat";

        String[] args = new String[]{
            "-Dimporttsv.input.format.class=pl.ceon.research.richimporttsv.io.SeparatorInputFormat",
            "-Dimporttsv.record.separator=#",
            "-Dimporttsv.separator=$",
            "-Dimporttsv.columns=HBASE_ROW_KEY," + COLUMN_FAMILY_NAME + ":" + COLUMN_QUALIFIER_NAME,
            tableInitName,
            inputFileName
        };

        HTable htableImport = doMROnTableTest(inputFileName, COLUMN_FAMILY_NAME, tableInitName, "KEY1$VALUE\n1#KEY2$VALUE2#\nKEY3$VALUE3", args);

        Result key1 = htableImport.get(new Get(Bytes.toBytes("KEY1")));
        assertNotNull(key1);
        assertEquals("VALUE\n1", Bytes.toString(key1.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER)));
        Result key3 = htableImport.get(new Get(Bytes.toBytes("\nKEY3")));
        assertNotNull(key3);
        assertEquals("VALUE3", Bytes.toString(key3.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER)));

        dropTable(tableInitName);
    }

    //@Test
    public void testDirRichImportTsv() throws Exception {
        String tableInitName = getCurrentDateAppended("testDirRichImportTsv");
        String inputFileName = "InputFile2.dat";
        String outputDirName = getCurrentDateAppended("richtsv-output");
        FileSystem dfs = util.getDFSCluster().getFileSystem();

        Path qualifiedOutputDir = dfs.makeQualified(new Path(outputDirName));
        assertFalse(dfs.exists(qualifiedOutputDir));

        // Prepare the arguments required for the test.
        String[] args = new String[]{
            "-Dimporttsv.input.format.class=pl.ceon.research.richimporttsv.io.SeparatorInputFormat",
            "-Dimporttsv.record.separator=#",
            "-Dimporttsv.separator=$",
            "-Dimporttsv.columns=HBASE_ROW_KEY," + COLUMN_FAMILY_NAME + ":" + COLUMN_QUALIFIER_NAME,
            "-Dimporttsv.bulk.output=" + outputDirName,
            tableInitName,
            inputFileName
        };

        doMROnTableTest(inputFileName, COLUMN_FAMILY_NAME, tableInitName, "KEY1$VALUE\n1#KEY2$VALUE2#\nKEY3$VALUE3", args);

        assertTrue(dfs.exists(qualifiedOutputDir));

        String localOutputDir = "src/test/resource/inputformat/output/" + outputDirName;
        dfs.copyToLocalFile(qualifiedOutputDir, new Path(localOutputDir));
    }

    private HTable doMROnTableTest(String inputFile, String family, String tableName, String line, String[] args) throws Exception {

        GenericOptionsParser opts = new GenericOptionsParser(util.getConfiguration(), args);
        Configuration config = util.getConfiguration();
        args = opts.getRemainingArgs();

        FileSystem fs = util.getDFSCluster().getFileSystem();
        FSDataOutputStream op = fs.create(new Path(inputFile), true);
        op.write(line.getBytes(HConstants.UTF8_ENCODING));
        op.close();

        assertTrue(fs.exists(new Path(inputFile)));

        final byte[] FAM = Bytes.toBytes(family);
        final byte[] TAB = Bytes.toBytes(tableName);

        HTable htableImport = util.createTable(TAB, FAM);
        assertEquals(0, util.countRows(htableImport));

        Job job = RichImportTsv.createSubmittableJob(config, args);
        job.waitForCompletion(false);
        assertTrue(job.isSuccessful());
        return htableImport;
    }

    //@Test(timeout = 1800000)
    public void testRowCounter() throws Exception {
        String tableInitName = getCurrentDateAppended("testRowCounter");
        createAndPopulateTable(tableInitName, TEST_ROW_COUNT);

        Job job = RowCounter.createSubmittableJob(util.getConfiguration(), new String[]{tableInitName});
        job.waitForCompletion(true);
        long count = job.getCounters().findCounter("org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters", "ROWS").getValue();
        Assert.assertEquals(TEST_ROW_COUNT, count);

        dropTable(tableInitName);
    }

    //@Test(timeout = 1800000)
    public void testCopy() throws Exception {

        String tableInitName = getCurrentDateAppended("testCopy");
        createAndPopulateTable(tableInitName, TEST_ROW_COUNT);

        final String tableCopyName = tableInitName + "Copy";
        HTable htableCopy = util.createTable(Bytes.toBytes(tableCopyName), COLUMN_FAMILY);

        Job job = CopyTable.createSubmittableJob(util.getConfiguration(), new String[]{"--new.name=" + tableCopyName, tableInitName});
        job.waitForCompletion(true);
        Assert.assertEquals(TEST_ROW_COUNT, (long) util.countRows(htableCopy));

        dropTable(tableInitName);
        dropTable(tableCopyName);
    }

    //@Test(timeout = 1800000)
    public void testExportImport() throws Exception {

        String tableInitName = getCurrentDateAppended("testExportImport");
        createAndPopulateTable(tableInitName, TEST_ROW_COUNT);

        FileSystem dfs = util.getDFSCluster().getFileSystem();
        Path qualifiedTempDir = dfs.makeQualified(new Path("export-import-temp-dir"));
        Assert.assertFalse(dfs.exists(qualifiedTempDir));

        Job jobExport = Export.createSubmittableJob(util.getConfiguration(), new String[]{tableInitName, qualifiedTempDir.toString()});
        jobExport.waitForCompletion(true);

        Assert.assertTrue(dfs.exists(qualifiedTempDir));

        final String tableImportName = tableInitName + "Import";
        HTable htableImport = util.createTable(Bytes.toBytes(tableImportName), COLUMN_FAMILY);

        Job jobImport = Import.createSubmittableJob(util.getConfiguration(), new String[]{tableImportName, qualifiedTempDir.toString()});
        jobImport.waitForCompletion(true);
        Assert.assertEquals(TEST_ROW_COUNT, (long) util.countRows(htableImport));

        dropTable(tableInitName);
        dropTable(tableImportName);
    }
}
