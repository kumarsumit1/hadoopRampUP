package com.impetus.hadoop.customeWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WebLogJob {
  public static void main(String[] args) throws Exception {
    String in =
        "/home/impadmin/Documents/workspaces/hadoop_ramp_wksp/rampup/weblog";
    String out =
        "/home/impadmin/Documents/workspaces/hadoop_ramp_wksp/rampup/output";

    if (args.length == 0) {
      args = new String[2];
      args[0] = in;
      args[1] = out;
    }
    Configuration conf = new Configuration();
    Job job = new Job();
    job.setJobName("WebLog Reader");

    job.setJarByClass(WebLogJob.class);

    job.setMapperClass(WebLogMapper.class);
    job.setReducerClass(WebLogReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapOutputKeyClass(WebLogWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
