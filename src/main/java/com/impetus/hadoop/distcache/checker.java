package com.impetus.hadoop.distcache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class checker extends Configured implements Tool {
  public static String localFile =
      "/home/hadoop/Desktop/data/TLD/google-ip-valid.txt";
  public static String hdfsFile = "/home/hadoop/googleBL/google-ip-valid.txt";

  public static class MapClass extends MapReduceBase implements
      Mapper<LongWritable, Text, Text, NullWritable> {

    private HashSet<String> blIP = new HashSet<String>();

    static String ValidIpAddressRegex =
        "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$";

    public void configure(JobConf job) {
      System.out.println("inside configure()");
      // blIP.add("98.202.86.206");
      try {
        Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);

        if (cacheFiles != null && cacheFiles.length > 0) {
          System.out.println("Inside setup(): " + cacheFiles[0].toString());

          String line;

          BufferedReader cacheReader =
              new BufferedReader(new FileReader(cacheFiles[0].toString()));
          try {
            while ((line = cacheReader.readLine()) != null) {
              blIP.add(line.trim());
            }
          } finally {
            cacheReader.close();
          }
        }
      } catch (IOException e) {
        System.err.println("Exception reading DistribtuedCache: " + e);
      }
    }

    public void map(LongWritable key, Text value,
        OutputCollector<Text, NullWritable> output, Reporter reporter)
        throws IOException {
      String[] fields = value.toString().split(" ");
      if (fields.length == 3) {

        String last = fields[2].trim();
        System.out.println(value + " : " + last);
        if (last.matches(ValidIpAddressRegex) && blIP.contains(last)) {
          output.collect(new Text(value), NullWritable.get());
        }
      }
    }
  }

  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    JobConf job = new JobConf(conf, checker.class);

    // Path cacheFile = new Path(args[2]);
    // DistributedCache.addCacheFile(cacheFile.toUri(), job);

    this.cacheFile(job);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    job.setMapperClass(MapClass.class);
    job.setNumReduceTasks(1);
    // job.setReducerClass(Reduce.class);

    job.setInputFormat(TextInputFormat.class);
    job.setOutputFormat(TextOutputFormat.class);

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);

    JobClient.runJob(job);

    return 0;
  }

  void cacheFile(JobConf conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path hdfsPath = new Path(hdfsFile);

    // upload the file to hdfs. Overwrite any existing copy.
    fs.copyFromLocalFile(false, true, new Path(localFile), hdfsPath);

    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
  }

  /**
   * The main method.
   * 
   * @param args the arguments
   * @throws Exception the exception
   */
  public static void main(String[] args) throws Exception {
    String in = "/home/hadoop/testTLD";
    String out = "/home/hadoop/tld-ip-bl";

    if (args.length == 0) {
      args = new String[2];
      args[0] = in;
      args[1] = out;
    }

    int res = ToolRunner.run(new Configuration(), new checker(), args);

    System.exit(res);
  }
}