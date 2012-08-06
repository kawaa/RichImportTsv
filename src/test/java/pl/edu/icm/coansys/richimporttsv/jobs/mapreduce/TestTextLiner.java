package pl.edu.icm.coansys.richimporttsv.jobs.mapreduce;

import java.io.File;
import java.util.Date;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTextLiner {

    private static final Log LOG = LogFactory.getLog(TestTextLiner.class);
    private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

    private String getCurrentDateAppended(String name) {
        return name + "-" + new Date().getTime();
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UTIL.startMiniCluster();
        UTIL.startMiniMapReduceCluster();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        UTIL.shutdownMiniMapReduceCluster();
        UTIL.shutdownMiniCluster();
    }

    @Test(timeout = 1800000)
    public void testTextLiner() throws Exception {

        // generate unique names
        String prefix = getCurrentDateAppended("testTextLiner");
        String inputDirName = prefix + "-input";
        String outputDirName = prefix + "-output";

        FileSystem dfs = UTIL.getDFSCluster().getFileSystem();

        // create qualified names
        Path qualifiedInputDir = dfs.makeQualified(new Path(inputDirName));
        Path qualifiedOutputDir = dfs.makeQualified(new Path(outputDirName));

        // create input direcotry and copy input data
        Assert.assertTrue(dfs.mkdirs(qualifiedInputDir));
        dfs.copyFromLocalFile(new Path("src/test/resource/input/richimporttsv/plain_hash.dat"), qualifiedInputDir);

        // run mapreduce job
        ToolRunner.run(UTIL.getConfiguration(), new TextLiner(), new String[]{"-Drecord.separator=#", qualifiedInputDir.toString(), qualifiedOutputDir.toString()});

        // copy output to local directory
        String localOutputDir = "src/test/resource/output/inputformat/" + prefix;
        dfs.copyToLocalFile(qualifiedOutputDir, new Path(localOutputDir));

        // check diff
        File localOutputFile = new File(localOutputDir + "/part-m-00000");
        File localExpectedFile = new File("src/test/resource/exp/richimporttsv/text_liner_plain_hash.exp");
        boolean isEqual = FileUtils.contentEquals(localExpectedFile, localOutputFile);
        Assert.assertTrue(isEqual);
    }
}
