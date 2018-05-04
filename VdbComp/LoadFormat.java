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
import Utils.*;

/**
 *
 */
public class LoadFormat
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private static Vector luns_from_format = new Vector(64, 0);

  private static JTable left_table;
  private static JTable right_table;

  private static SdLeft  left_model;
  private static SdRight right_model;

  private static String  slice = "s6";

  /**
   * format output already can be found in a file.
   */
  public static Vector readFromFile()
  {
    luns_from_format.removeAllElements();
    //String fname = askForFile();
    String fname = "a";
    if (fname == null)
      return new Vector(0);

    String[] lines = Fget.readFileToArray(fname);
    for (int i = 0; i < lines.length; i++)
      processLine(lines[i], "stdout");

    if (luns_from_format.size() == 0)
      common.failure("No valid devices found.");

    //buildTables(false);
    //
    return luns_from_format;
  }


  /**
   * Ask user for a file name to be used
   */
  private static String askForFile()
  {

    JFileChooser fc = new JFileChooser((new File (".").getAbsolutePath()));
    fc.setFileSelectionMode( JFileChooser.FILES_ONLY );

    if (fc.showOpenDialog(null) == fc.APPROVE_OPTION)
    {
      String file = fc.getSelectedFile().getAbsolutePath();
      return file;
    }

    return null;
  }




  /**
   * Parse format output and pick update the lun names
   */
  private static void processLine(String line, String type)
  {
    //line = line.toLowerCase();
    if (type.equals("stdout") && line.indexOf("<") != -1)
    {
      //common.ptod(type + ": " + line);
      StringTokenizer st = new StringTokenizer(line);
      String disk = st.nextToken();
      String lun  = st.nextToken();
      String target = "n/a";

      st = new StringTokenizer(lun, "ct");
      if (st.countTokens() > 0)
        target = st.nextToken();


      SdFormat fm = new SdFormat();
      fm.disk_number = disk.substring(0, disk.indexOf("."));
      fm.lun    = lun;
      fm.target = target;
      luns_from_format.add(fm);
    }
  }


  public static JTable buildRightTable(Vector luns_from_format)
  {
    right_model = new SdRight(luns_from_format);
    right_table = new JTable(right_model);

    right_table.addMouseListener(new java.awt.event.MouseAdapter()
                                 {
                                   public void mouseClicked(MouseEvent e)
                                   {
                                     if (e.getClickCount() > 1)
                                     {
                                       moveRightToLeft();
                                     }
                                   }
                                 });

    setColumnWidth(right_table);
    right_table.setFont(new Font("Courier New", Font.PLAIN, 12));

    return right_table;
  }



  public static JTable buildLeftTable()
  {
    left_model = new SdLeft(new Vector(64, 0));
    left_table = new JTable(left_model);
    left_table.addMouseListener(new java.awt.event.MouseAdapter()
                                {
                                  public void mouseClicked(MouseEvent e)
                                  {
                                    if (e.getClickCount() > 1)
                                    {
                                      moveLeftToRight();
                                    }
                                  }
                                });


    left_table.setFont(new Font("Courier New", Font.PLAIN, 12));

    return left_table;
  }




  /**
   * Move selected luns from the right panel to the left panel
   */
  public static void moveRightToLeft()
  {

    /* Pick up all the selected row numbers: */
    int[] rows = right_table.getSelectedRows();
    for (int i= 0; i < rows.length; i++)
    {
      int row = rows[i];
      SdFormat fm = right_model.getRow(row);

      /* Add them to the SD list: */
      left_model.addRow(fm);
    }

    /* Remove them from the lun list: */
    right_model.removeRows(rows);

    left_model.fireTableStructureChanged();
    right_model.fireTableStructureChanged();
    setColumnWidth(left_table);
    setColumnWidth(right_table);
  }



  /**
   * Move selected luns from the left panel to the right panel
   */
  public static void moveLeftToRight()
  {
    /* Pick up all the selected row numbers: */
    int[] rows = left_table.getSelectedRows();
    for (int i= 0; i < rows.length; i++)
    {
      int row = rows[i];
      SdFormat fm = left_model.getRow(row);

      /* Add them to the lun list: */
      right_model.addRow(fm);
    }

    /* Remove them from the SD list: */
    left_model.removeRows(rows);

    left_model.fireTableStructureChanged();
    right_model.fireTableStructureChanged();
    setColumnWidth(left_table);
    setColumnWidth(right_table);
  }




  /**
   * Set the minimum column width for each column
   */
  private static void setColumnWidth(JTable table)
  {
    TableColumnModel model = table.getColumnModel();
    for (int i = 0; i < table.getColumnCount(); i++)
    {
      TableWidth.sizeColumn(i, table);

      if (table == right_table)
      {
        if (i == 0)
        {
          //table.getColumnModel().getColumn(0).setMinWidth(1);
          //table.getColumnModel().getColumn(0).setMaxWidth(200);
        }

        else
        {
          FractionCellRenderer rend = new FractionCellRenderer(5, 1, SwingConstants.RIGHT);
          model.getColumn(i).setCellRenderer(rend);
          //table.getColumnModel().getColumn(i).setMaxWidth(30);
        }
      }
    }
  }

  public static String getSlice()
  {
    return slice;
  }
  public static void storeSlice(String sl)
  {
    slice = sl;
    left_model.fireTableStructureChanged();
    right_model.fireTableStructureChanged();
    setColumnWidth(left_table);
    setColumnWidth(right_table);
  }
}
