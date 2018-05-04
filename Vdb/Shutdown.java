package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import Utils.OS_cmd;


/**
 * This class is a Shutdown hook.
 *
 * There are two uses:
 * - send a message to both stderr and stdout so that a JVM that is waiting for
 *   it's completion in OS_cmd() gets a message, allowing any outstanding
 *   BufferedReader.readLine() to complete.
 *
 * - Close any open report files. This is cheaper than using flush() for each
 *   single report line written. With many luns and many slaves doing the flush
 *   each time for each line for each interval just gets to be too expensive.
 */
public class Shutdown extends Thread
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private Shutdown()
  {
    setName("Shutdown");
  }

  public static void activateShutdownHook()
  {
    Runtime.getRuntime().addShutdownHook(new Shutdown());
  }



  public void run()
  {

    if (SlaveJvm.isThisSlave())
    {
      System.out.println(OS_cmd.getShutdownMessage());
      System.err.println(OS_cmd.getShutdownMessage());
    }

    Report.closeAllReports();
  }
}
