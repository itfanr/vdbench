package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.PrintWriter;


import java.io.*;
import java.util.*;
import Utils.printf;


/**
 * This class handles the collection of size information for each active anchor.
 *
 * The Slave at some point in time will put the needed info in an instance of
 * this class, and then send it on to the master who will report it in the
 * report.
 */
public class AnchorReport implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private String name;
  private long   files;
  private long   dirs;
  private long   size;
  private long   total_files;
  private long   total_dirs;
  private long   total_size;
  private long   wss_files;
  private long   wss_dirs;
  private long   wss_size;
  private long   now_files;
  private long   now_dirs;
  private long   now_size;
  private long   files_opened;
  private long   size_opened;

  private static String anchor_mask;
  private static Report anchor_report;


  public AnchorReport(String name,
                      long   files,        long dirs,       long size,
                      long   total_files,  long total_dirs, long total_size,
                      long   wss_files,    long wss_dirs,   long wss_size,
                      long   now_files,    long now_dirs,   long now_size,
                      long   files_opened, long size_opened)
  {
    this.name         = name;
    this.files        = files;
    this.dirs         = dirs;
    this.size         = size;
    this.total_files  = total_files;
    this.total_dirs   = total_dirs;
    this.total_size   = total_size;
    this.wss_files    = wss_files;
    this.wss_dirs     = wss_dirs;
    this.wss_size     = wss_size;
    this.now_files    = now_files;
    this.now_dirs     = now_dirs;
    this.now_size     = now_size;
    this.files_opened = files_opened;
    this.size_opened = size_opened;

    SlaveJvm.sendMessageToMaster(SocketMessage.ANCHOR_SIZES, this);
  }


  public synchronized void printNumbers()
  {
    printf pf = new printf(anchor_mask + "%8d %8d %6s %8d %8d %6s %8d %8d %6s %8d %8d %6s %8d %6s");
    pf.add(name);
    pf.add(files);
    pf.add(dirs);
    pf.add(FileAnchor.whatSize1(size));
    pf.add(total_files);
    pf.add(total_dirs);
    pf.add(FileAnchor.whatSize1(total_size));
    pf.add(wss_files);
    pf.add(wss_dirs);
    pf.add(FileAnchor.whatSize1(wss_size));
    pf.add(now_files);
    pf.add(now_dirs);
    pf.add(FileAnchor.whatSize1(now_size));
    pf.add(files_opened);
    pf.add(FileAnchor.whatSize1(size_opened));
    anchor_report.println(pf.print());
  }


  /**
   * It's not pretty, but it works.
   */
  public static Report create()
  {
    printf pf;
    anchor_report = new Report("anchors", "Anchor status report");

    Report.getSummaryReport().printHtmlLink("Link to anchor report",
                                            anchor_report.getFileName(), "anchors");

    anchor_report.println("(This report will not contain useful info when the "+
                          "'shared=yes' FSD parameter is used)\n");

    setMaxAnchorMask();

    pf = new printf(anchor_mask + "   %-24s %-24s %-24s %-24s %-24s ");
    pf.add("");
    pf.add("......Anchor size....");
    pf.add("......Total size.....");
    pf.add("......Workingset.....");
    pf.add("....Existing ........");
    pf.add("...Opened...");
    anchor_report.println(pf.print());

    pf = new printf(anchor_mask + "%8s %8s %6s %8s %8s %6s %8s %8s %6s %8s %8s %6s %8s %6s");
    pf.add("");
    pf.add("files");
    pf.add("dirs");
    pf.add("bytes");
    pf.add("files");
    pf.add("dirs");
    pf.add("bytes");
    pf.add("files");
    pf.add("dirs");
    pf.add("bytes");
    pf.add("files");
    pf.add("dirs");
    pf.add("bytes");
    pf.add("files");
    pf.add("bytes");
    anchor_report.println(pf.print());
    anchor_report.println("");
    return anchor_report;
  }

  /**
   * Save hroizontal space by calculating the maximum anchor name in bytes and
   * creating a mask for it.
   */
  private static void setMaxAnchorMask()
  {
    Vector anchors = FileAnchor.getAnchorList();
    int width = 0;
    for (int i = 0; i < anchors.size(); i++)
    {
      FileAnchor anchor = (FileAnchor) anchors.elementAt(i);
      width = Math.max(width, anchor.getAnchorName().length());
    }
    anchor_mask = "%-" + width + "s ";
  }
}

/*
                          anchor size            total size             wss size             exist size                 opened size
anchor                dirs    files  maxsize dirs    files  maxsize  dirs    files  maxsize  dirs    files  maxsize dirs    files  maxsize
*/
