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

import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumnModel;
import java.text.NumberFormat;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import Vdb.common;



/**
 * Cell Renderer handles the content and color of the data displayed in JTable
 */
class FractionCellRenderer extends DefaultTableCellRenderer
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  protected int integer;
  protected int fraction;
  protected int align;
  protected NumberFormat formatter = NumberFormat.getInstance();

  public FractionCellRenderer(int integer, int fraction, int align)
  {
    this.integer  = integer;
    this.fraction = fraction;
    this.align    = align;
  }

  public Component getTableCellRendererComponent(JTable table,
                                                 Object value,
                                                 boolean isSelected,
                                                 boolean hasFocus,
                                                 int row,
                                                 int column)
  {

    super.getTableCellRendererComponent(table, value, isSelected,
                                        hasFocus, row, column);
    if ((value != null) && (value instanceof Number))
    {
      formatter.setMaximumIntegerDigits(integer);
      formatter.setMaximumFractionDigits(fraction);
      formatter.setMinimumFractionDigits(fraction);
      formatter.setGroupingUsed(false);
      setText(formatter.format(((Number) value).doubleValue()));
    }

    else if (value instanceof DeltaValue)
    {
      setBackground(((DeltaValue) value).color);
      setValue(((DeltaValue) value).delta_value);
    }

    else
      setValue(value);

    setHorizontalAlignment(align);

    return this;
  }
}

