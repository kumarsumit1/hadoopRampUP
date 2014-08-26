package com.impetus.hadoop.movilens2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TextWritable implements Writable {
  public String x;
  public String y;
  public String z;

  public TextWritable(String x, String y, String z) {
    this.x = x;
    this.y = y;
    this.z = z;
  }

  public TextWritable() {
    this("", "", "");
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, x);
    WritableUtils.writeString(out, y);
    WritableUtils.writeString(out, z);

  }

  public void readFields(DataInput in) throws IOException {

    x = WritableUtils.readString(in);
    y = WritableUtils.readString(in);
    z = WritableUtils.readString(in);
  }

  public String toString() {
    return x + ", " + y + ", " + z;
  }

}
