package VdbComp;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.*;
import java.util.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.table.*;
import Utils.common;
import Utils.Format;

/**
 * JTable data model for the workload comparator
 */
public class SdRight extends AbstractTableModel
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String[]     column_names;
  private String[]     solaris = { "lun", "s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7" };
  private String[]     windows = { "Drive #", "Drive letters" };
  private String[]     linux   = { "Lun", "Size" };

  private Vector       luns; /* Lun names from format */

  public SdRight(Vector lunlist)
  {
    if (common.onLinux())
      column_names = linux;

    else if (common.onWindows())
      column_names = windows;

    else
      column_names = solaris;


    luns = lunlist;
    Collections.sort(luns, new SortFormat());
  }

  public void addRow(SdFormat fm)
  {
    luns.add(fm);
    Collections.sort(luns, new SortFormat());
  }

  public void removeRows(int[] rows)
  {
    for (int i = 0; i < rows.length; i++)
      luns.set(rows[i], null);

    while (luns.remove(null));
  }


  public Object getValueAt(int row, int col)
  {
    SdFormat fm = (SdFormat) luns.elementAt(row);

    long KB = 1024l;
    long MB = 1024l * 1024l;
    long GB = 1024l * 1024l * 1024l;
    long TB = 1024l * 1024l * 1024l * 1024l;

    if (common.onWindows())
    {
      if (col == 0)
        return fm.lun;
      else if (col == 1)
        return fm.slices;
      else
        common.failure("Invalid column requested");
    }

    if (common.onLinux())
    {
      if (col == 0)
        return fm.lun;
      else if (col == 1)
        return fm.slices;
      else
        common.failure("Invalid column requested");
    }

    if (col == 0)
      return fm.lun;
    else
    {
      double size = fm.sizes[col - 1];
      if (size == 0)
        return "";
      else if (size > TB)
        return Format.f("%.2ft", (size / TB));
      else if (size > GB)
        return Format.f("%.2fg", (size / GB));
      else if (size > MB)
        return Format.f("%.2fm", (size / MB));
      else if (size > KB)
        return Format.f("%.2fk", (size / KB));
      else
        return Format.f("%.2f", (size));
    }
  }
  public SdFormat getRow(int row)
  {
    return (SdFormat) luns.elementAt(row);
  }


  public int getRowCount()
  {
    return luns.size();
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
