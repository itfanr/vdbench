package VdbComp;

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

import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.*;

import javax.swing.*;
import javax.swing.table.*;

import Utils.Fget;
import Utils.Fput;
import Utils.common;

/**
 * Workload Comparator menu options.
 */
public class WlMenus extends JMenuBar implements ActionListener
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private WlComp wlcomp;


  private JMenu             file_menu  = new JMenu("File");
  private JMenu             options    = new JMenu("Options");

  private static JCheckBoxMenuItem show_mb    = new JCheckBoxMenuItem("Show MB/sec");
  private JMenuItem         ranges     = new JMenuItem("Modify Color Ranges");
  private JMenuItem         save       = new JMenuItem("Export ");
  private JMenuItem         exit       = new JMenuItem("Exit");

  public WlMenus(WlComp wl)
  {
    wlcomp = wl;

    add(file_menu);
    add(options);

    show_mb.addActionListener(this);
    ranges.addActionListener(this);
    exit.addActionListener(this);
    save.addActionListener(this);

    options.add(show_mb);
    options.add(ranges);
    file_menu.add(save);
    file_menu.add(exit);
  }


  public void actionPerformed(ActionEvent e)
  {
    String cmd = e.getActionCommand();

    if (cmd.equals(exit.getText()))
    {
      StoredParms.storeParms();
      System.exit(0);
    }

    else if (cmd.equals(ranges.getText()))
    {
      ChangeRanges cr = new ChangeRanges(wlcomp);
      cr.setSize(200, 300);
      Utils.Message.centerscreen(cr);
      cr.setVisible(true);
    }

    else if (cmd.equals(show_mb.getText()))
    {
      wlcomp.clearTable();
    }

    else if (cmd.equals(save.getText()))
      doExport();
  }


  public static boolean showMB()
  {
    return show_mb.isSelected();
  }


  private void doExport()
  {
    Vector old_runs = wlcomp.old_runs;
    Vector new_runs = wlcomp.new_runs;
    DataModel dm = wlcomp.dm;

    String file_name = wlcomp.getFile();
    if (file_name == null)
      return;

    Fput fp = new Fput(file_name);
    fp.println("Directory\t" +
               "rd\t" +
               "oldreqrate\t" +
               "newreqrate\t" +
               "oldresp\t" +
               "newresp\t" +
               "deltaresp\t" +
               "oldrate\t" +
               "newrate\t" +
               "deltarate\t" +
               "oldmb\t" +
               "newmb\t" +
               "deltamb\t");

    for (int i = 0; i < old_runs.size(); i++)
    {
      Run old_run = (Run) old_runs.elementAt(i);
      Run new_run = (Run) new_runs.elementAt(i);

      String line = old_run.getSubDir();
      line += "\t" + old_run.rd_name;
      line += "\t" + (Double) old_run.flatfile_data.get("reqrate");
      line += "\t" + (Double) new_run.flatfile_data.get("reqrate");
      line += "\t" + old_run.flatfile_data.get("resp");
      line += "\t" + new_run.flatfile_data.get("resp");
      line += "\t" + new DeltaValue(dm.getDelta(old_run, new_run, "resp")).delta_value;
      line += "\t" + old_run.flatfile_data.get("rate");
      line += "\t" + new_run.flatfile_data.get("rate");
      line += "\t" + new DeltaValue(dm.getDelta(old_run, new_run, "rate")).delta_value;
      line += "\t" + old_run.flatfile_data.get("MB/sec");
      line += "\t" + new_run.flatfile_data.get("MB/sec");
      line += "\t" + new DeltaValue(dm.getDelta(old_run, new_run, "MB/sec")).delta_value;
      fp.println(line);
    }

    fp.close();
  }
}

  /*
    if (column_names[col].equalsIgnoreCase("Subdirectory"))
      return old_run.getSubDir();

    else if (column_names[col].equalsIgnoreCase("Run"))
      return old_run.rd_name;


    else if (column_names[col].equalsIgnoreCase("Old iorate"))
    {
      Double num = (Double) old_run.flatfile_data.get("reqrate");
      if (num.doubleValue() == Vdb.RD_entry.MAX_RATE)
        return "max";
      else if (num.doubleValue() == Vdb.RD_entry.CURVE_RATE)
        return "curve";
      return num;
    }

    else if (column_names[col].equalsIgnoreCase("New iorate"))
    {
      Double num = (Double) new_run.flatfile_data.get("reqrate");
      if (num.doubleValue() == Vdb.RD_entry.MAX_RATE)
        return "max";
      else if (num.doubleValue() == Vdb.RD_entry.CURVE_RATE)
        return "curve";
      return num;
    }


    else if (column_names[col].equalsIgnoreCase("Old resp"))
      return old_run.flatfile_data.get("resp");

    else if (column_names[col].equalsIgnoreCase("New resp"))
      return  new_run.flatfile_data.get("resp");

    else if (column_names[col].equalsIgnoreCase("Delta resp"))
      return new DeltaValue(getDelta(old_run, new_run, "resp"));


    else if (column_names[col].equalsIgnoreCase("Old iops"))
      return  old_run.flatfile_data.get("rate");

    else if (column_names[col].equalsIgnoreCase("New iops"))
      return  new_run.flatfile_data.get("rate");

    else if (column_names[col].equalsIgnoreCase("Delta iops"))
      return new DeltaValue(getDelta(old_run, new_run, "rate"));


    else if (column_names[col].equalsIgnoreCase("Old mbs"))
      return  old_run.flatfile_data.get("MB/sec");

    else if (column_names[col].equalsIgnoreCase("New mbs"))
      return  new_run.flatfile_data.get("MB/sec");

    else if (column_names[col].equalsIgnoreCase("Delta mbs"))
      return new DeltaValue(getDelta(old_run, new_run, "MB/sec"));
   */
