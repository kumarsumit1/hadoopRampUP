package com.impetus.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RegexUDF extends UDF {
  public Text evaluate(Text input) {

    if (input == null)
      return new Text("");
    return new Text("=\"" + input.toString() + "\"");
  }
}
