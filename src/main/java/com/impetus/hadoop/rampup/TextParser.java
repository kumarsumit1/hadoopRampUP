package com.impetus.hadoop.rampup;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class TextParser extends Configured implements Tool{
      public int run(String[] args) throws Exception
      {
            //creating a JobConf object and assigning a job name for identification purposes
            JobConf conf = new JobConf(getConf(), TextParser.class);
            conf.setJobName("TextParser");

            //Setting configuration object with the Data Type of output Key and Value
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(IntWritable.class);

            //Providing the mapper and reducer class names
            conf.setMapperClass(WordCountMapper.class);
            conf.setReducerClass(WordCountReducer.class);
            //We wil give 2 arguments at the run time, one in input path and other is output path
            Path inp = new Path(args[0]);
            Path out = new Path(args[1]);
            //the hdfs input and output directory to be fetched from the command line
            FileInputFormat.addInputPath(conf, inp);
            FileOutputFormat.setOutputPath(conf, out);
             
          //  conf.
            
            JobClient.runJob(conf);
            return 0;
      }
     
      public static void main(String[] args) throws Exception
      {
    	// Create configuration
          Configuration conf = new Configuration(true);
          
       // this should be like defined in your yarn-site.xml
       //   conf.set("yarn.resourcemanager.address", "yarn-manager.com:50001"); 

          // framework is now "yarn", should be defined like this in mapred-site.xm
       //   conf.set("mapreduce.framework.name", "yarn");

          // like defined in hdfs-site.xml
       //   conf.set("fs.default.name", "hdfs://namenode.com:9000");
          
            // this main function will call run method defined above.
        int res = ToolRunner.run(conf, new TextParser(),args);
            System.exit(res);
      }
}