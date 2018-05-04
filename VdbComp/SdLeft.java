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
public class SdLeft extends AbstractTableModel
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String[]     column_names = { "Generated output"};

  private Vector       luns;

  public SdLeft(Vector lunlist)
  {
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
    String mask       = SdBuild.getMask();
    String left_mask  = mask;
    String right_mask = null;
    SdFormat fm = (SdFormat) luns.elementAt(row);


    if (mask.indexOf("<") != -1)
    {
      left_mask  = mask.substring(0, mask.indexOf("<"));
      left_mask  = common.replace(left_mask, "<", "");
      right_mask = mask.substring(mask.indexOf("<"));
      right_mask = common.replace(right_mask, "<", "");
      right_mask = common.replace(right_mask, ">", "");
      //common.ptod("left_mask: " + left_mask);
      //common.ptod("right_mask: " + right_mask);
    }


    /* Normal processing: */
    if (mask.indexOf("<") == -1)
    {
      String line = left_mask;
      line = common.replace(line, "#", ("" + (row+1)));
      line = common.replace(line, "$", fm.lun);
      line = common.replace(line, "%", SdBuild.getSlice());
      return line;
    }

    /* Row0: */
    if (row == 0)
    {
      String line = left_mask;
      line = common.replace(line, "#", ("" + (row+1)));
      line = common.replace(line, "$", fm.lun);
      line = common.replace(line, "%", SdBuild.getSlice());

      line += right_mask;
      line  = common.replace(line, "#", ("" + (row+1)));
      line  = common.replace(line, "$", fm.lun);
      line  = common.replace(line, "%", SdBuild.getSlice());

      /* Unless the last row, add '\': */
      if (row != luns.size() -1)
        line += " \\";
      return line;
    }

    /* All other rows: */
    String line = right_mask;
    line = common.replace(line, "#", ("" + (row+1)));
    line = common.replace(line, "$", fm.lun);
    line = common.replace(line, "%", SdBuild.getSlice());

    /* Unless the last row, add '\': */
    if (row != luns.size() -1)
      line += " \\";
    return line;
  }

  public SdFormat getRow(int row)
  {
    return(SdFormat) luns.elementAt(row);
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
