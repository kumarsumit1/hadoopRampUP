package com.impetus.hadoop.wordCount;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WcJob {

  public static void main(String... args) {
    try {
      System.out.println(" !!! job Started !!!!");
      String input = "/user/impadmin/wcount";
      // "/home/impadmin/Documents/workspaces/hadoop_ramp_wksp/rampup/input";
      String output =
          "/home/impadmin/Documents/software/hadoop-1.0.4/gutenberg-output";
      // "/home/impadmin/Documents/workspaces/hadoop_ramp_wksp/rampup/output";
      JobConf jc = new JobConf(WcJob.class);
      jc.setJobName("WcJob");

      Path hdfsPath =
          new Path("/user/impadmin/movielens/movierating/wordlist.txt");
      DistributedCache.addCacheFile(hdfsPath.toUri(), jc);

      jc.setOutputKeyClass(Text.class);
      jc.setOutputValueClass(IntWritable.class);

      jc.setMapperClass(WcMapper.class);
      jc.setCombinerClass(WcReducer.class);
      jc.setReducerClass(WcReducer.class);

      jc.setInputFormat(TextInputFormat.class);
      jc.setOutputFormat(TextOutputFormat.class);

      FileInputFormat.setInputPaths(jc, new Path(input));
      FileOutputFormat.setOutputPath(jc, new Path(output));

      JobClient.runJob(jc);

      System.out.println("Job Execution finished");

    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
