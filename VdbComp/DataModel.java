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

import Utils.Format;
import Utils.common;

import Vdb.RD_entry;

/**
 * JTable data model for the workload comparator
 */
public class DataModel extends AbstractTableModel
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private String[]               column_names = null;
  private FractionCellRenderer[] renderers = null;
  private Vector                 old_runs;
  private Vector                 new_runs;

  public DataModel(WlComp wl)
  {
    createcolumn_names(wl);
    old_runs = wl.old_runs;
    new_runs = wl.new_runs;

    /* Remove an rd=format_for_ run: */
    for (int i = 0; i < old_runs.size(); i++)
    {
      Run run = (Run) old_runs.elementAt(i);
      if (run.rd_name.startsWith(RD_entry.FORMAT_RUN))
        old_runs.set(i, null);
    }
    while (old_runs.remove(null));
    for (int i = 0; i < new_runs.size(); i++)
    {
      Run run = (Run) new_runs.elementAt(i);
      if (run.rd_name.startsWith(RD_entry.FORMAT_RUN))
        new_runs.set(i, null);
    }
    while (new_runs.remove(null));
  }


  /**
   * Create column headers.
   */
  public void createcolumn_names(WlComp wl)
  {
    /* Create the column_names: */
    String[] keywords = (String[]) wl.all_keywords.values().toArray(new String[0]);
    int cols = (WlMenus.showMB()) ? 13 + keywords.length * 2 : 10 + keywords.length * 2;

    column_names = new String[ cols ];
    renderers    = new FractionCellRenderer[ cols ];

    /* Fixed column names: */
    int idx = 0;
    addColumn(idx++, "Subdirectory", 0, 0, SwingConstants.LEFT);
    addColumn(idx++, "Run",          0, 0, SwingConstants.LEFT);

    /* Variable 'forxx' column names: */
    for (int i = 0; i < keywords.length; i++)
    {
      addColumn(idx++, "Old " + keywords[i], 8, 0, SwingConstants.RIGHT);
      addColumn(idx++, "New " + keywords[i], 8, 0, SwingConstants.RIGHT);
    }

    /* Data column names: */
    addColumn(idx++, "Old iorate",  8, 0, SwingConstants.RIGHT);
    addColumn(idx++, "New iorate",  8, 0, SwingConstants.RIGHT);
    addColumn(idx++, "Old resp",    8, 3, SwingConstants.RIGHT);
    addColumn(idx++, "New resp",    8, 3, SwingConstants.RIGHT);
    addColumn(idx++, "Delta resp",  8, 3, SwingConstants.RIGHT);
    addColumn(idx++, "Old iops",    8, 0, SwingConstants.RIGHT);
    addColumn(idx++, "New iops",    8, 0, SwingConstants.RIGHT);
    addColumn(idx++, "Delta iops",  8, 0, SwingConstants.RIGHT);

    /* Optional column names: */
    if (WlMenus.showMB())
    {
      addColumn(idx++, "Old mbs",     8, 1, SwingConstants.RIGHT);
      addColumn(idx++, "New mbs",     8, 1, SwingConstants.RIGHT);
      addColumn(idx++, "Delta mbs",   8, 1, SwingConstants.RIGHT);
    }
  }


  /**
   * Add a column, together with renderer information.
   */
  private void addColumn(int idx, String col, int width, int dec, int align)
  {
    column_names[idx] = col;
    renderers[idx]    = new FractionCellRenderer(width, dec, align);
  }


  /**
   * Set display renders for easch column
   */
  public void setRenderers(JTable table)
  {
    TableColumnModel model = table.getColumnModel();
    for (int i = 0; i < column_names.length; i++)
      model.getColumn(i).setCellRenderer(renderers[i]);
  }


  /**
   * Set the minimum column width for each column
   */
  public void setColumnWidth(JTable table)
  {
    TableColumnModel model = table.getColumnModel();
    for (int i = 0; i < column_names.length; i++)
      TableWidth.sizeColumn(i, table);
  }


  public Object getValueAt(int row, int col)
  {
    Run old_run = (Run) old_runs.elementAt(row);
    Run new_run = (Run) new_runs.elementAt(row);

    if (column_names[col].equalsIgnoreCase("Subdirectory"))
      return old_run.getSubDir();

    else if (column_names[col].equalsIgnoreCase("Run"))
      return old_run.rd_name;


    else if (column_names[col].equalsIgnoreCase("Old iorate"))
    {
      Double num = (Double) old_run.flatfile_data.get("reqrate");
      if (num.doubleValue() == Vdb.RD_entry.MAX_RATE ||
          num.doubleValue() == 99999998)
        return "max";
      else if (num.doubleValue() == Vdb.RD_entry.CURVE_RATE ||
               num.doubleValue() == 99999997)
        return "curve";
      return num;
    }

    else if (column_names[col].equalsIgnoreCase("New iorate"))
    {
      Double num = (Double) new_run.flatfile_data.get("reqrate");
      if (num.doubleValue() == Vdb.RD_entry.MAX_RATE ||
          num.doubleValue() == 99999998)
        return "max";
      else if (num.doubleValue() == Vdb.RD_entry.CURVE_RATE ||
               num.doubleValue() == 99999997)
        return "curve";
      return num;
    }


    /* Response time values and delta: */
    else if (column_names[col].equalsIgnoreCase("Old resp"))
      return old_run.flatfile_data.get("resp");

    else if (column_names[col].equalsIgnoreCase("New resp"))
      return  new_run.flatfile_data.get("resp");

    else if (column_names[col].equalsIgnoreCase("Delta resp"))
      return new DeltaValue(getDelta(old_run, new_run, "resp"));


    /* Iops time values and delta: */
    else if (column_names[col].equalsIgnoreCase("Old iops"))
      return  old_run.flatfile_data.get("rate");

    else if (column_names[col].equalsIgnoreCase("New iops"))
      return  new_run.flatfile_data.get("rate");

    else if (column_names[col].equalsIgnoreCase("Delta iops"))
      return new DeltaValue(getDelta(old_run, new_run, "rate"));


    /* MB/sec time values and delta: */
    else if (column_names[col].equalsIgnoreCase("Old mbs"))
      return  old_run.flatfile_data.get("MB/sec");

    else if (column_names[col].equalsIgnoreCase("New mbs"))
      return  new_run.flatfile_data.get("MB/sec");

    else if (column_names[col].equalsIgnoreCase("Delta mbs"))
      return new DeltaValue(getDelta(old_run, new_run, "MB/sec"));


    else if (column_names[col].startsWith("Old "))
      return old_run.forxx_values.get(column_names[col].substring(4));

    else if (column_names[col].startsWith("New "))
      return new_run.forxx_values.get(column_names[col].substring(4));

    else
    {
      Object obj = old_run.forxx_values.get(column_names[col]);
      if (obj == null)
        return "n/a";

      return obj;
    }
  }


  /**
   * Get the delta percentage between two values
   */

  public double getDelta(Run old_run, Run new_run, String label)
  {
    if (old_run.flatfile_data.get(label) == null)
      common.failure("can not find label: " + label);
    double old_val = ((Double) old_run.flatfile_data.get(label)).doubleValue();
    double new_val = ((Double) new_run.flatfile_data.get(label)).doubleValue();
    double delta    = old_val - new_val;

    if (label.equals("resp"))
      return delta * 100. / old_val;
    else
      return delta * 100. / old_val * -1;

  }

  public int getRowCount()
  {
    return old_runs.size();
  }

  public int getColumnCount()
  {
    return column_names.length;
  }
  public String getColumnName(int col)
  {
    return column_names[col];
  }
}
