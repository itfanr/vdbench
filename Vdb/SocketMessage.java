package Vdb;

/*
 * Copyright 2010 Sun Microsystems, Inc. All rights reserved.
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * The contents of this file are subject to the terms of the Common
 * Development and Distribution License("CDDL") (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at http://www.sun.com/cddl/cddl.html
 * or ../vdbench/license.txt. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice
 * in each file and include the License file at ../vdbench/licensev1.0.txt.
 *
 * If applicable, add the following below the License Header, with the
 * fields enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */


/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;

/**
 * This class contains the Object that gets passed back and forth between
 * sockets.
 */
class SocketMessage implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private int    message_number;
  private Object data_object;
  private long   quick_info;
  private int    message_seqno;
                                                      //   master  slave
  public static int SEND_SIGNON_INFO_TO_MASTER = 1;   //          ><
  public static int SEND_SIGNON_SUCCESSFUL     = 2;   //          >
  public static int KILL_SLAVE_SIGNON_ERROR    = 3;   //          >
  public static int GET_LUN_INFO_FROM_SLAVE    = 4;   //
  public static int ERROR_MESSAGE              = 5;   //
  public static int COUNT_ERRORS               = 6;   //
  public static int WORK_TO_SLAVE              = 7;   //          >
  public static int CONFIRM_WORK_TO_MASTER     = 8;   //             unused
  public static int REQUEST_SLAVE_STATISTICS   = 9;   //          >
  public static int SLAVE_STATISTICS           = 10;  //          <
  public static int SLAVE_WORK_COMPLETED       = 11;  //
  public static int CLEAN_SHUTDOWN_SLAVE       = 12;  //
  public static int RSH_COMMAND                = 13;  //
  public static int RSH_STDOUT_OUTPUT          = 14;  //
  public static int RSH_STDERR_OUTPUT          = 15;  //
  public static int PERSISTENCE_BLOCK_COUNT    = 16;  //
  public static int SLAVE_ABORTING             = 17;  //
  public static int MASTER_ABORTING            = 18;  //
  public static int SLAVE_REACHED_EOF          = 19;  //
  public static int SLAVE_READY_TO_GO          = 20;  //
  public static int SLAVE_GO                   = 21;  //
  public static int WORKLOAD_DONE              = 22;  //
  public static int CLEAN_SHUTDOWN_COMPLETE    = 23;  //
  public static int CONSOLE_MESSAGE            = 24;  //
  public static int ADM_MESSAGES               = 25;  //
  public static int HEARTBEAT_MESSAGE          = 26;  //
  public static int STARTING_FILE_STRUCTURE    = 27;  //
  public static int ENDING_FILE_STRUCTURE      = 28;  //
  public static int READY_FOR_MORE_WORK        = 29;  //
  public static int ANCHOR_SIZES               = 30;  //


  private static String[] text=
  {
    "Unknown"
    ,"SEND_SIGNON_INFO_TO_MASTER"
    ,"SEND_SIGNON_SUCCESSFUL    "
    ,"KILL_SLAVE_SIGNON_ERROR   "
    ,"GET_LUN_INFO_FROM_SLAVE   "
    ,"ERROR_MESSAGE             "
    ,"COUNT_ERRORS              "
    ,"WORK_TO_SLAVE             "
    ,"CONFIRM_WORK_TO_MASTER    "
    ,"REQUEST_SLAVE_STATISTICS  "
    ,"SLAVE_STATISTICS          "
    ,"SLAVE_WORK_COMPLETED      "
    ,"CLEAN_SHUTDOWN_SLAVE      "
    ,"RSH_COMMAND               "
    ,"RSH_STDOUT_OUTPUT         "
    ,"RSH_STDERR_OUTPUT         "
    ,"PERSISTENCE_BLOCK_COUNT   "
    ,"SLAVE_ABORTING            "
    ,"MASTER_ABORTING           "
    ,"SLAVE_REACHED_EOF         "
    ,"SLAVE_READY_TO_GO         "
    ,"SLAVE_GO                  "
    ,"WORKLOAD_DONE             "
    ,"CLEAN_SHUTDOWN_COMPLETE   "
    ,"CONSOLE_MESSAGE           "
    ,"ADM_MESSAGES              "
    ,"HEARTBEAT_MESSAGE         "
    ,"STARTING_FILE_STRUCTURE   "
    ,"ENDING_FILE_STRUCTURE     "
    ,"READY_FOR_MORE_WORK       "
    ,"ANCHOR_SIZES              "
  };

  private static int seqno = 0;
  private static Object sequence_lock = new Object();

  public SocketMessage(int message)
  {
    this.message_number = message;
  }

  public SocketMessage(int message, Object data)
  {
    this.message_number = message;
    this.data_object    = data;
  }

  public void setData(Object data)
  {
    data_object = data;
  }
  public void setInfo(long info)
  {
    quick_info = info;
  }
  public Object getData()
  {
    return data_object;
  }
  public long getInfo()
  {
    return quick_info;
  }
  public int getMessageNum()
  {
    return message_number;
  }
  public String getMessageText()
  {
    return text[message_number];
  }

  public void setSeqno()
  {
    synchronized (sequence_lock)
    {
      message_seqno = seqno++;
    }
  }


  public int getSeqno()
  {
    return message_seqno;
  }

}
