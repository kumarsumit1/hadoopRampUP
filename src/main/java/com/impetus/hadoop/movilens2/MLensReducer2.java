package com.impetus.hadoop.movilens2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MLensReducer2 extends
    Reducer<Text, IntWritable, Text, TextWritable> {
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int i = 0;
    int maxRate = Integer.MIN_VALUE;
    int minRate = Integer.MAX_VALUE;
    int sum = 0;
    for (IntWritable valCount : values) {
      i++;
      if (valCount.get() < minRate) {
        minRate = valCount.get();
      }
      if (maxRate < valCount.get()) {
        maxRate = valCount.get();
      }
      sum = sum + valCount.get();
    }

    if (i > 0)
      context.write(key, new TextWritable());
  }
}
