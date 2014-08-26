package com.impetus.hadoop.wordCount;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WcReducer extends MapReduceBase implements
    Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterator<IntWritable> valList,
      OutputCollector<Text, IntWritable> reducedData, Reporter rep1)
      throws IOException {
    int sum = 0;
    while (valList.hasNext()) {
      sum = sum + valList.next().get();
    }
    // if (sum > 5)
    reducedData.collect(key, new IntWritable(sum));
  }

}
