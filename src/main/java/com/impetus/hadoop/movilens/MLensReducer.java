package com.impetus.hadoop.movilens;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MLensReducer extends
    Reducer<Text, IntWritable, Text, NullWritable> {

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    int i = 0;
    for (IntWritable val : values) {
      sum = sum + val.get();
      i++;
    }
    ArrayWritable a = new ArrayWritable(IntWritable.class);
    a.set(new IntWritable[] { new IntWritable(sum), new IntWritable(sum / i) });

    String reqtxt = key.toString() + "  " + i + "  " + sum / i;
    // if(i>0)
    context.write(new Text(reqtxt), null);
  }

}
