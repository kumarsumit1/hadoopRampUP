package com.impetus.hadoop.wordCount;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WcMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, IntWritable> {
  private HashSet<String> wordList = new HashSet<String>();

  @Override
  public void map(LongWritable key, Text value,
      OutputCollector<Text, IntWritable> outputData, Reporter rep)
      throws IOException {
    // String[] keyArray = value.toString().split("\n");
    // Path[] myCacheFiles = DistributedCache.getLocalCacheFiles(job);
    // System.out.println(value.toString());
    // System.out.println(key);
    String[] keyArray = value.toString().split(" ");
    for (String keyitem : keyArray) {
      if (wordList.contains(keyitem)) {
        outputData.collect(new Text(keyitem), new IntWritable(1));
      } else {
        // System.out.println("ommiting word : " + keyitem);
      }
      // outputData.collect(new Text(keyitem), new IntWritable(1));
      // outputData.collect(new Text("count"), new IntWritable(1));
    }

  }

  public void configure(JobConf job) {
    System.out.println("inside configure()");
    // blIP.add("98.202.86.206");
    try {
      // Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
      URI[] cacheFiles = DistributedCache.getCacheFiles(job);

      if (cacheFiles != null && cacheFiles.length > 0) {
        System.out.println("Inside setup(): " + cacheFiles[0].toString());

        String line;

        BufferedReader cacheReader =
            new BufferedReader(new FileReader(cacheFiles[0].toString()));
        try {
          while ((line = cacheReader.readLine()) != null) {
            System.out.println("words added : " + line.trim());
            wordList.add(line.trim());

          }
        } finally {
          cacheReader.close();
        }
      }
    } catch (IOException e) {
      System.err.println("Exception reading DistribtuedCache: " + e);
    }
  }

}
