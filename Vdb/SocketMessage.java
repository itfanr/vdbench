package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;

/**
 * This class contains the Object that gets passed back and forth between
 * sockets.
 */
public class SocketMessage implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private int    message_number;
  private Object data_object;
  private long   quick_info;
  private int    message_seqno;

  /* These two fields are really only for debugging to see how long         */
  /* the socket transfer takes. Field SlaveSocket.shortest_delta is used to */
  /* serve as a 'base' if clocks on both sides are not in sync.             */
  /* (I don't think that sync works correctly)                              */
  public long    send_time;
  public long    receive_time;
  //                                                        master  slave
  public static int SEND_SIGNON_INFO_TO_MASTER =  1;  //          ><
  public static int SEND_SIGNON_SUCCESSFUL     =  2;  //          >
  public static int KILL_SLAVE_SIGNON_ERROR    =  3;  //          >
  public static int GET_LUN_INFO_FROM_SLAVE    =  4;  //
  public static int ERROR_MESSAGE              =  5;  //          <
  public static int COUNT_ERRORS               =  6;  //
  public static int WORK_TO_SLAVE              =  7;  //          >
  public static int ERROR_LOG_MESSAGE          =  8;  //          <
  public static int REQUEST_SLAVE_STATISTICS   =  9;  //          >
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
  public static int SUMMARY_MESSAGE            = 25;  //
  public static int ADM_MESSAGES               = 26;  //
  public static int HEARTBEAT_MESSAGE          = 27;  //
  public static int STARTING_FILE_STRUCTURE    = 28;  //
  public static int ENDING_FILE_STRUCTURE      = 29;  //
  public static int READY_FOR_MORE_WORK        = 30;  //
  public static int ANCHOR_SIZES               = 31;  //
  public static int USER_DATA_TO_SLAVES        = 32;  //


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
    ,"SUMMARY_MESSAGE           "
    ,"ADM_MESSAGES              "
    ,"HEARTBEAT_MESSAGE         "
    ,"STARTING_FILE_STRUCTURE   "
    ,"ENDING_FILE_STRUCTURE     "
    ,"READY_FOR_MORE_WORK       "
    ,"ANCHOR_SIZES              "
    ,"USER_DATA_TO_SLAVES       "
  };

  private static int seqno = 0;
  private static Object sequence_lock = new Object();

  public static int SIGNON_INFO_SIZE = 6;


  public SocketMessage(int message)
  {
    this.message_number = message;
  }

  public SocketMessage(int message, Object data)
  {
    message_number = message;
    data_object = data;
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
