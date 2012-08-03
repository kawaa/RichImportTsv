/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pl.edu.icm.coansys.richimporttsv.jobs.mapreduce;

/**
 *
 * @author akawa
 */
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import pl.edu.icm.coansys.richimporttsv.io.SeparatorInputFormat;

public class TextLiner extends Configured implements Tool {

  
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
      

        Job job = new Job(conf, TextLiner.class.getSimpleName());
        job.setNumReduceTasks(0);

        job.setInputFormatClass(SeparatorInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextLiner(), args);
        System.exit(res);
    }
}