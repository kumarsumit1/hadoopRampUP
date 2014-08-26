package com.impetus.hadoop.movilens;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MLensJob extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    String in = "/user/impadmin/movielens/movierating/u.data";
    String out = "/user/impadmin/movielens/output";

    if (args.length == 0) {
      args = new String[2];
      args[0] = in;
      args[1] = out;
    }
    int exitCode = ToolRunner.run(new Configuration(), new MLensJob(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] arg) throws Exception {
    URI hdfsUri = new URI("/user/impadmin/movielens/movierating/u.item");

    Job job = new Job();
    Configuration conf = job.getConfiguration();
    job.setJobName("Movie lens total and avg rating");
    DistributedCache.addCacheFile(hdfsUri, conf);
    job.setJarByClass(MLensJob.class);

    job.setMapperClass(MLensMapper.class);
    job.setReducerClass(MLensReducer.class);

    // job.setMapOutputKeyClass(MovieCalogueVO.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(arg[0]));
    FileOutputFormat.setOutputPath(job, new Path(arg[1]));

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

}
