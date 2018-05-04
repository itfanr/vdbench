package VdbGui;

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

/**
 * <p>Title: EntryTable.java</p>
 * <p>Description: This class displays selected entries and ensures that
 * cells are not editable.</p>
 * @author Jeff Shafer
 * @version 1.0
 */

import javax.swing.JTable;

public class EntryTable extends JTable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  /**
   * Overrides the default constructor.
   * @param row the number of rows in the table.
   * @param column the number of columns in the table.
   */
  public EntryTable(int row, int column)
  {
    super(row, column);
  }

  /**
   * The default version of this method is overridden here to specifically
   * prevent the user from being able to edit the values in the data table.
   * @param row the row index of a cell.
   * @param col the column index of a cell.
   * @return the boolean value false, indicating that no cells are editable.
   */
  public boolean isCellEditable(int row, int col)
  {
    return false;
  }
}
