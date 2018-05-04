package VdbComp;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.Vector;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.swing.*;
import javax.swing.JTable;
import java.awt.Insets;
import Vdb.common;


/**
 * Size the data with for a table column.
 */
class TableWidth
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  /**
   * Automatically size the columns to the largest element.
   */
  public static void sizeColumn(int col, JTable tbl)
  {
    int largest = 0;

    /* Loop thru rows: */
    for (int i = 0; i < tbl.getRowCount(); ++i)
    {
      Object value = tbl.getValueAt(i, col);
      if (!(value instanceof String))
          continue;

      String str = (String) value;

      /* Only store the largest size for tbl: */
      largest = Math.max(largest, tbl.getFontMetrics(tbl.getFont()).stringWidth(str));
    }

    if (tbl.getRowCount() == 0)
      return;

    Insets insets = ((JComponent)tbl.getCellRenderer (0, col)).getInsets();

    largest += insets.left + insets.right + 5;
    largest  = Math.max(largest, 25);

    tbl.getColumnModel().getColumn(col).setMinWidth(largest);
    //common.ptod("size_column(): " + largest + " " + insets.left + " " + insets.right);
  }

}
