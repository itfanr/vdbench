package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.*;

class Monday
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  static int    processes           = 6;
  static int    threads_per_process = 10;
  static int    files               = 1800;
  static long   base_file_size      = 100*2024*1024;
  static String dir                 = "/dir";


  /*
  dir=/dir,processes=6,threads=20,files=1800,size=(100m,5m,10m)

  results in /dir1,/dir2,../dir6
  10 threads for each dir/process.
  */
  public static void setup(String parms)
  {

    for (int i = 0; i < processes; i++)
    {
      MondayProcess mp = new MondayProcess(dir+i, 1800);

      for (int j = 0; j < threads_per_process; j++)
      {
        MondayThread thread = new MondayThread(mp);
      }
    }
  }
}



class MondayProcess
{
  String dir;
  int    files;
  int    next_file_to_use;

  public MondayProcess(String dir, int files)
  {
    this.dir = dir;
    this.files = files;
  }

  public synchronized String[] getSomeFiles(int count)
  {
    if (next_file_to_use >= files)
      return null;

    String[] file_names = new String[count];
    for (int i = 0; i < count; i++, next_file_to_use++)
    {
      file_names[i] = dir + File.separator + "file" + (next_file_to_use);
    }

    return file_names;
  }
}



class MondayThread extends Thread
{
  MondayProcess mp;

  public MondayThread(MondayProcess mp)
  {
    this.mp = mp;
  }

  public void run()
  {
    mp.getSomeFiles(30);

    /*

    I'll need FileEntry and Directory instances to allow code sharing.
    Using ActiveFile requires me to have an anchor?
    I could define 'n' anchors and that will then be the process count?
    I can even have the anchor contain 1800 FileEntries and that then can give
    me the file count and the file size!

    All the non-100mb files can just be fileentries outside of the anchor.
    I don't think they have to be in the anchor.

    I can use the FwgEntry belonging to this thread (needed by ActiveFile())

    ActiveFile does o/c/r/w, I can add a copy() if needed.


    */


  }
}

