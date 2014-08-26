package com.impetus.hadoop.movilens;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class MovieRatingVO extends ArrayWritable {

  public MovieRatingVO() {
    super(IntWritable.class);
  }

}
