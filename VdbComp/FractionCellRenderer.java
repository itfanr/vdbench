package VdbComp;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
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
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

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

