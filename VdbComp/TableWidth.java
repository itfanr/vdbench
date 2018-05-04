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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";



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
