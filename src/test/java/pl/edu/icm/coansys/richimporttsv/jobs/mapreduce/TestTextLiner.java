package pl.edu.icm.coansys.richimporttsv.jobs.mapreduce;

import java.io.File;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

public class TestTextLiner extends GenericTestCase {

    private static final Log LOG = LogFactory.getLog(TestTextLiner.class);
  
    
    //@Test(timeout = 1800000)
    public void testTextLiner() throws Exception {

        // generate unique names
        String prefix = getCurrentDateAppended("testTextLiner");
        String inputDirName = prefix + "-input";
        String outputDirName = prefix + "-output";

        FileSystem dfs = util.getDFSCluster().getFileSystem();

        // create qualified names
        Path qualifiedInputDir = dfs.makeQualified(new Path(inputDirName));
        Path qualifiedOutputDir = dfs.makeQualified(new Path(outputDirName));

        // create input direcotry and copy input data
        Assert.assertTrue(dfs.mkdirs(qualifiedInputDir));
        dfs.copyFromLocalFile(new Path("src/test/resource/inputformat/input/plain_hash.dat"), qualifiedInputDir);

        // run mapreduce job
        ToolRunner.run(util.getConfiguration(), new TextLiner(), new String[]{"-Drecord.separator=#", qualifiedInputDir.toString(), qualifiedOutputDir.toString()});

        // copy output to local directory
        String localOutputDir = "src/test/resource/inputformat/output/" + prefix;
        dfs.copyToLocalFile(qualifiedOutputDir, new Path(localOutputDir));

        // check diff
        File localOutputFile = new File(localOutputDir + "/part-m-00000");
        File localExpectedFile = new File("src/test/resource/inputformat/exp/text_liner_plain_hash.exp");
        boolean isEqual = FileUtils.contentEquals(localExpectedFile, localOutputFile);
        Assert.assertTrue(isEqual);
    }
}
