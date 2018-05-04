package Vdb;

/*
 *
 * Copyright (c) 2000-2008 Sun Microsystems, Inc. All Rights Reserved.
 *
 */

import java.util.*;


/*
 * Code related to using PDH (Performance Data Helper) C functions.
 */
public class WindowsPDH
{
  private final static String c = "Copyright (c) 2000-2008 Sun Microsystems, Inc. " +
                                  "All Rights Reserved.";

  private static String load_once = common.get_shared_lib();


  private static long pdh_query = 0;


  /**
   * Passing a null length to this method turns DEBUG displays on.
   */
  static native String translateFieldName(String field);
  public static String translateFieldNameOptional(String field)
  {
    String ret = translateFieldName(field);
    if (ret == null)
      return field;
    return ret;
  }


  static native long createQuery(String[] query_string);

  static native String getQueryData(long query_anchor);

  static native String expandCounterPath(String path);


  public static void main2(String[] args)
  {
    String translated = WindowsPDH.translateFieldName("Redirector");
    common.ptod("translated: " + translated);

    translated = WindowsPDH.translateFieldName("Bytes Received/sec");
    common.ptod("translated: " + translated);

    String queries[] = new String[]
    {
      "\\Processor(_Total)\\% Processor Time",
      "\\Processor(_Total)\\% DPC Time",
      "\\Processor(_Total)\\% Interrupt Time",
      "\\Processor(_Total)\\% User Time"
    };

    if ((pdh_query = createQuery(queries)) < 0)
      common.failure("Error creating Windows PDH query");

    String data = getQueryData(pdh_query);
    common.ptod("data: " + data);

    for (int i = 0; i < 10; i++)
    {
      common.sleep_some(1000);

      data = getQueryData(pdh_query);
      common.ptod("data: " + data);
    }
  }


  public static void main3(String[] args)
  {
    String network = WindowsPDH.translateFieldName("Network Interface");
    String total   = WindowsPDH.translateFieldName("Bytes Total/sec");
    //common.ptod("network: " + network);
    //common.ptod("total: " + total);

    String ret = expandCounterPath("\\" + network + "(*)\\" + total);

    //common.ptod("ret: " + ret);

    String[] split = ret.split("\\$");
    for (int i = 0; i < split.length; i++)
    {
      common.ptod("split: " + split[i]);
      StringTokenizer st = new StringTokenizer(split[i], "\\()");
      while (st.hasMoreTokens())
      {
        common.ptod("token: " + st.nextToken());
      }
    }
  }


  /**
   * Return a list of disk drives.
   */
  public static String[] getDisks(String type)
  {
    Vector disks    = new Vector( 8,0);
    //WindowsPDH.translateFieldName("");
    String disktype = WindowsPDH.translateFieldName(type);
    String total    = WindowsPDH.translateFieldName("% Disk Time");

    String ret = expandCounterPath("\\" + disktype + "(*)\\" + total);
    if (ret == null)
    {
      common.ptod("No windows '" + type + "' disks found.");
      return new String[0];
    }

    //common.ptod("ret: " + ret);

    String[] split = ret.split("\\$");
    for (int i = 0; i < split.length; i++)
    {
      //common.ptod("split: " + split[i]);
      StringTokenizer st = new StringTokenizer(split[i], "\\()");
      st.nextToken();
      st.nextToken();
      String disk = st.nextToken();
      if (!disk.equals("_Total"))
        disks.add(disk);
    }

    return(String[]) disks.toArray(new String[0]);
  }

  public static void main4(String[] args)
  {
    String[] field_names =
    {
      translateFieldNameOptional("% Disk Time"),
      translateFieldNameOptional("Avg. Disk Queue Length"),
      translateFieldNameOptional("Avg. Disk sec/Transfer"),
      translateFieldNameOptional("Disk Read Bytes/sec"   ),
      translateFieldNameOptional("Disk Write Bytes/sec"  ),
      translateFieldNameOptional("Disk Reads/sec"        ),
      translateFieldNameOptional("Disk Writes/sec"       )
    };

    String   type   = translateFieldNameOptional("PhysicalDisk");
    String[] disks  = getDisks(type);
    Vector   list   = new Vector(16, 0);

    for (int i = 0; i < disks.length; i++)
    {
      common.ptod("disks: " + disks[i]);
    }

    System.exit(0);

    for (int i = 0; i < disks.length; i++)
    {
      //if (! disks[i].startsWith("1"))
      //  continue;
      String array[] = new String[ field_names.length ];
      for (int j = 0; j < field_names.length; j++)
      {

        String name = "\\" + type + "(" + disks[i] + ")\\" +  field_names[j];
        //common.ptod("name: " + name);
        array[j] = name;

      }
      long query = createQuery(array);
      //common.ptod("query: " + query);

      String data = WindowsPDH.getQueryData(query);
      for (int x = 0; x < 100; x++)
      {
        common.sleep_some(1000);
        data = WindowsPDH.getQueryData(query);
        common.ptod("data1: " + disks[i] + " " + data);
      }
    }
  }


  public static void main(String[] args)
  {
    String[] disks = getDiskList();

    for (int i = 0; i < disks.length; i++)
    {
      common.ptod("disks: " + disks[i]);
    }
  }

  public static String[] getDiskList()
  {

    String   type   = translateFieldNameOptional("PhysicalDisk");
    String[] disks  = getDisks(type);
    Vector   list   = new Vector(16, 0);

    for (int i = 0; i < disks.length; i++)
      list.add(disks[i]);

    return (String[]) list.toArray(new String[0]);
  }
}

