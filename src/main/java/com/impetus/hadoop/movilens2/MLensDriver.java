package com.impetus.hadoop.movilens2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MLensDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    String in = "/home/impadmin/Documents/workspaces/movieLens/u.data";
    String out = "/home/impadmin/Documents/workspaces/movieLens/output";

    if (args.length == 0) {
      args = new String[2];
      args[0] = in;
      args[1] = out;
    }
    int exitCode = ToolRunner.run(new Configuration(), new MLensDriver(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] arg) throws Exception {

    Job job = new Job();

    job.setJobName("users and the number of ratings");

    job.setJarByClass(MLensDriver.class);

    job.setMapperClass(MLensMapper2.class);
    job.setReducerClass(MLensReducer2.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TextWritable.class);

    /*
     * job.setInputFormat(TextInputFormat.class);
     * job.setOutputFormat(TextOutputFormat.class);
     */

    FileInputFormat.addInputPath(job, new Path(arg[0]));
    FileOutputFormat.setOutputPath(job, new Path(arg[1]));

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

}
