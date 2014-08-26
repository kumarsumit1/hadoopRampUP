package com.impetus.hadoop.movilens2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MLensMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] words = value.toString().split("\\s+");
    context.write(new Text(words[0]),
        new IntWritable(Integer.parseInt(words[2])));
  }
}
