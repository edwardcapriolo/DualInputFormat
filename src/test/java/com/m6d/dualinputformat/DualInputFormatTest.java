package com.m6d.dualinputformat;

import com.jointhegrid.hive_test.HiveTestService;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;

public class DualInputFormatTest extends HiveTestService {

  public DualInputFormatTest() throws IOException {
    super();
  }

  //add test to the name if you want to run this.
  public void Execute() throws Exception {
    Path p = new Path(this.ROOT_DIR, "afile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("1\n");
    bw.write("2\n");
    bw.close();

    String jarFile = DualInputFormat.class.getProtectionDomain()
             .getCodeSource().getLocation().getFile();

    //these do not work with hive thrift we have to get the
    ///files into auxlib
    //DistributedCache.addCacheFile( new URI(jarFile), new Configuration());
    //client.execute( "set hive.aux.jars.path="+new URI(jarFile).toASCIIString());
    //do not know how to work around this for now build and put in hadoop_home/lib... yuk

    client.execute ( "add jar "+jarFile);
    client.execute("create table  dual  (fake string) "+
            "STORED AS INPUTFORMAT 'com.m6d.dualinputformat.DualInputFormat'"+
            "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");
    client.execute("load data local inpath '" + p.toString() + "' into table dual");
    client.execute("select count(1) as cnt from dual");
    String row = client.fetchOne();
    assertEquals( "1", row);

    client.execute("select * from dual");
    row = client.fetchOne();
    assertEquals( "", row);

    client.execute("drop table dual");
  }


}