package Vdb;
import java.io.File;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

/**
  * The common class contains some general service methods
  *
  * Warning: some calls from code in the Utils package to similary named methods
  * here will NOT actually use the code below!
  * Need to prevent that some day.
  */
public class SizeOf
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  public static void main(String args[]) throws Exception
  {
    sizeof(args);
  }

  /**
   * sizeof
   *
   * Minimum 8 bytes for any even empty instance.
   **/
  public static void sizeof(String args[]) throws Exception
  {
    int loops = 2000000;
    if (args.length > 0)
      loops = Integer.parseInt(args[0]) * 1000000;
    Object[] sink = new Object[loops];

    /* Make sure we have no old garbage: */
    System.gc();
    System.gc();
    double used_at_start = Runtime.getRuntime().totalMemory() -
                           Runtime.getRuntime().freeMemory();


    for (int i = 0; i < loops; i++)
    {
      //sink[i] = new Directory();
      //sink[i] = new FwdCounter(null);
      sink[i] = new Histogram("default");

      //((Directory) sink[i]).debugging =
      //  "/net/sbm-240a.us.oracle.com/export/henk-adp-test/fsd1/"+
      //  "vdb.1_5.dir/vdb.2_1.dir/vdb.3_10.dir/vdb.4_9.dir/vdb.5_9.dir/vdb.6_1.dir/vdb.7_6.dir" + i;


      //sink[i] = new File(
      //"/net/sbm-240a.us.oracle.com/export/henk-adp-test/fsd1/"+
      //"vdb.1_5.dir/vdb.2_1.dir/vdb.3_10.dir/vdb.4_9.dir/vdb.5_9.dir/vdb.6_1.dir/vdb.7_6.dir" + i);;

    }


    System.gc();
    System.gc();


    Jmap.runJmap();

    long total_heap = Jmap.getTotalHeap();
    common.ptod("bytes per loop: " + (total_heap / loops));

    /* This code is here to assure that GC can not clean up the sink array yet: */
    long dummy = 0;
    for (int i = 0; i < loops; i++)
      dummy += sink[i].hashCode();
  }
}
