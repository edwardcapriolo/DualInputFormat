DualInputFormat
===============

An input format that returns a single split with a single row.

Can be used from hive like this.

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
