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
package pl.edu.icm.coansys.richimporttsv.jobs.mapreduce;

import java.io.File;
import java.io.IOException;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;

import java.util.Date;
import org.apache.hadoop.fs.FileUtil;

public class GenericTestCase {

    private Log LOG = LogFactory.getLog(GenericTestCase.class);
    protected HBaseTestingUtility util;
    protected int NUM_REGIONSERVERS = 1;
    protected int NUM_SLAVES = 1;
    // some prefefinied names
    final protected long TEST_ROW_COUNT = 100;
    final protected String BASE_ROW = "row";
    final protected String COLUMN_FAMILY_NAME = "cf";
    final protected String COLUMN_QUALIFIER_NAME = "cq";
    final protected byte[] COLUMN_FAMILY = Bytes.toBytes(COLUMN_FAMILY_NAME);
    final protected byte[] COLUMN_QUALIFIER = Bytes.toBytes(COLUMN_QUALIFIER_NAME);
    final protected byte[] VALUE = Bytes.toBytes("value");

    protected void initialize() throws Exception {
        //System.setProperty("hadoop.log.dir", "test-logs");

        Configuration conf = new Configuration();

        conf.set("hbase.regionserver.info.port", "-1");
        conf.setInt("mapred.submit.replication", 2);
        conf.set("dfs.datanode.address", "0.0.0.0:0");
        conf.set("dfs.datanode.http.address", "0.0.0.0:0");
        conf.set("mapred.map.max.attempts", "2");
        conf.set("mapred.reduce.max.attempts", "2");
        conf.set("dfs.max.objects", "111111111111");

        util = new HBaseTestingUtility(conf);

        util.getConfiguration().set("hbase.master.info.port", "60011");
        util.getConfiguration().set("hbase.regionserver.info.port", "60031");
        util.cleanupTestDir();

        util.startMiniCluster(NUM_REGIONSERVERS);

        util.getConfiguration().set("hadoop.log.dir", "build/test/logs");
        util.startMiniMapReduceCluster(NUM_REGIONSERVERS);

        // add close hook
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                util.shutdownMiniMapReduceCluster();
                try {
                    util.shutdownMiniCluster();
                } catch (Exception ex) {
                    LOG.info("Hook for shutdownMiniCluster fails: " + ex.getMessage());
                }
            }
        });
    }

    @Before
    public void beforeTest() throws Exception {
        initialize();
    }

    @After
    public void tearDown() throws Exception {
        FileUtil.fullyDelete(new File(util.getConfiguration().get("hadoop.tmp.dir")));
        util.shutdownMiniMapReduceCluster();
        util.shutdownMiniCluster();
    }

    public void dropTable(String tableName) {
        try {
            util.deleteTable(Bytes.toBytes(tableName));
        } catch (IOException ex) {
            LOG.info("Table can not be deleted: " + tableName);
        }
    }

    public HTable createAndPopulateTable(String tableName, long rowCount) throws IOException {
        HTable htable = util.createTable(Bytes.toBytes(tableName), COLUMN_FAMILY);
        for (int i = 0; i < rowCount; ++i) {
            Put put = new Put(Bytes.toBytes(BASE_ROW + i));
            put.add(COLUMN_FAMILY, COLUMN_QUALIFIER, VALUE);
            htable.put(put);
        }
        return htable;
    }

    public String getCurrentDateAppended(String name) {
        return name + "-" + new Date().getTime();
    }
}
