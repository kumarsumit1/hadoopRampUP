package com.impetus.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class GenericUDFS extends GenericUDF {

  @Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arg0)
      throws UDFArgumentException {
    // TODO Auto-generated method stub
    return null;
  }

}
