package Vdb;

/*
 * Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * A report file that tells the user, usually for automation purposes, what the
 * current state is of a Vdbench run.
 *
 * Report: status.html
 */
public class Status
{
  private final static String c =
  "Copyright (c) 2000, 2014, Oracle and/or its affiliates. All rights reserved.";

  private static Report status_report = null;

  private static SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy-HH:mm:ss-zzz ");

  public Status()
  {
    if (status_report != null)
      common.failure("Recursive creation of 'status.html'");

    status_report = new Report("status", "* Vdbench status");

    Report.getSummaryReport().printHtmlLink("Vdbench status", "status", "status");

    status_report.println("* The objective of this file is to contain easily parseable " +
                          "information about the current state of Vdbench.");
    status_report.println("* This then can serve as an 'official' interface for any software " +
                          "monitoring Vdbench.");
    status_report.println("* Each line of output will be immediately flushed to the file system, " +
                          "making its content accessible by any monitoring program.");
    status_report.println("* The values below are all tab-delimited.");
    status_report.println("");

    status_report.getWriter().flush();
  }


  public static void printStatus(String state, RD_entry rd)
  {
    /* This situation may exist with one of the many vdbench utilities: */
    if (status_report == null)
      return;

    String tod = df.format(new Date());
    String txt;

    if (rd != null)
      txt = String.format("%s\trd=%s\t%s", state, rd.rd_name, rd.current_override.getText());
    else
      txt = String.format("%s", state);

    String tmp = String.format("%s\t%s", tod, txt);
    status_report.println(tmp);

    status_report.getWriter().flush();

    // debugging:
    //common.ptod("status: " + tmp);
  }

}

