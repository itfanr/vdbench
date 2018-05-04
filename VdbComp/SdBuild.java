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

import java.io.*;
import java.util.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.table.*;
import Utils.*;
import Vdb.WindowsPDH;

/**
 *
 */
public class SdBuild extends JFrame implements ActionListener
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static Vector luns_from_format  = new Vector(64, 0);
  private static Vector lines_from_format = new Vector(64, 0);

  private static JPanel     top_panel  = null;
  private static JButton    do_fdisk  = new JButton("Run fdisk");
  private static JButton    do_fdiskr  = new JButton("Use fdisk output");
  private static JButton    do_format  = new JButton("Run format w/prtvtoc");
  private static JButton    do_formatn = new JButton("Run format");
  private static JButton    do_windows = new JButton("List disk drives");
  private static JButton    do_read    = new JButton("Use format output");
  private static JButton    replace    = new JButton("Replace parmfile");
  private static JButton    go_left    = new JButton("<<<<<");
  private static JButton    go_right   = new JButton(">>>>>");
  private static JButton    do_exit    = new JButton("Exit");
  private static JButton    do_save    = new JButton("Save");
  private static JLabel     mask_lbl   = new JLabel("Mask:"); //  "     Mask (#=seqno $=lun %=slice):");
  private static JComboBox  mask       = null;
  private static JLabel     slice_lbl  = new JLabel("     Slice:");
  private static JTextField slice      = new JTextField("s6");

  private static JTextField status_bar = new JTextField();

  private static JPanel bottom = null;

  public static JScrollPane left_panel  = null;
  public static JScrollPane right_panel = null;

  private JTable left_table;
  private JTable right_table;

  private SdLeft left_model;
  private SdRight right_model;


  /**
    * Build the TOP part of the window with all the buttons.
    */
  public SdBuild()
  {
    setTitle("Vdbench SD parameter generation tool");
    addWindowListener(new WindowAdapter()
                      {
                        public void windowClosing(WindowEvent e)
                        {
                          System.exit(0);
                        }
                      });

    mask = createComboBox();
    mask.setEditable(true);

    Dimension dim = new Dimension(1024, 20);
    status_bar.setFont(new Font("Courier New", Font.PLAIN, 14));
    status_bar.setBackground(SystemColor.activeCaptionBorder);
    status_bar.setMinimumSize(dim);
    status_bar.setPreferredSize(dim);
    status_bar.setMaximumSize(dim);

    dim = new Dimension(250, 20);
    mask.setMinimumSize(dim);
    mask.setPreferredSize(dim);
    mask.setMaximumSize(dim);

    do_fdisk  .addActionListener(this);
    do_fdiskr  .addActionListener(this);
    do_format  .addActionListener(this);
    do_formatn .addActionListener(this);
    do_windows .addActionListener(this);
    do_read    .addActionListener(this);
    replace    .addActionListener(this);
    go_left    .addActionListener(this);
    go_right   .addActionListener(this);
    do_exit    .addActionListener(this);
    do_save    .addActionListener(this);
    mask       .addActionListener(this);

    do_save.setEnabled(false);

    if (!common.onSolaris())
    {
      do_format.setEnabled(false);
      do_formatn.setEnabled(false);
    }

    if (top_panel != null)
      getContentPane().remove(top_panel);

    int x = 0;
    int y = 4;
    top_panel = new JPanel();
    top_panel.setLayout(new GridBagLayout());
    top_panel.add(do_exit,    new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    top_panel.add(do_save,    new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

    if (common.onLinux())
    {
      top_panel.add(do_fdisk,   new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
      top_panel.add(do_fdiskr,  new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    }
    else if (common.onSolaris())
    {
      top_panel.add(do_format,  new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
      top_panel.add(do_formatn, new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
      top_panel.add(do_read,    new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    }
    else if (common.onWindows())
    {
      top_panel.add(do_windows,  new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    }

    top_panel.add(go_left,    new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    top_panel.add(go_right,   new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    top_panel.add(replace,    new GridBagConstraints(x++, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    top_panel.add(mask_lbl,   new GridBagConstraints(y++, 1, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    top_panel.add(mask,       new GridBagConstraints(y++, 1, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    if (common.onSolaris())
    {
      top_panel.add(slice_lbl,  new GridBagConstraints(y++, 1, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
      top_panel.add(slice,      new GridBagConstraints(y++, 1, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    }

    getContentPane().add(top_panel,  BorderLayout.NORTH);
    getContentPane().add(status_bar, BorderLayout.SOUTH);
  }


  public static String getSlice()
  {
    return slice.getText();
  }
  public static String getMask()
  {
    return(String) mask.getSelectedItem();
  }


  /**
   *  Someone's pushing my buttons!
   */
  public void actionPerformed(ActionEvent e)
  {
    String cmd = e.getActionCommand();

    if (cmd.equals(do_exit.getText()))
      System.exit(0);

    else if (cmd.equals(go_left.getText()))
      moveRightToLeft();

    else if (cmd.equals(go_right.getText()))
      moveLeftToRight();

    else if (cmd.equals(do_format.getText()))
      buildTables(true, true);

    else if (cmd.equals(do_formatn.getText()))
      buildTables(true, false);

    else if (cmd.equals(do_read.getText()))
      readFormatFromFile();

    else if (cmd.equals(replace.getText()))
      replaceParms();

    else if (cmd.equals(do_save.getText()))
      saveToFile();

    else if (cmd.equals("comboBoxChanged") && left_table != null)
      ((AbstractTableModel) left_table.getModel()).fireTableDataChanged();

    else if (cmd.equals(do_windows.getText()))
      getWindowsDiskList();

    else if (cmd.equals(do_fdiskr.getText()))
      readFdiskFromFile(null);

    else if (cmd.equals(do_fdisk.getText()))
      readFdiskFromFile(doFdisk());

    else
      common.failure("Invalid command: " + cmd);

    this.setVisible(true);
    //this.repaint();
  }


  /**
   * format output already can be found in a file.
   */
  private void readFormatFromFile()
  {
    String fname = askForFile();
    if (fname == null)
      return;

    String[] lines = Fget.readFileToArray(fname);
    for (int i = 0; i < lines.length; i++)
      processFormatLine(lines[i], "stdout", false);

    if (luns_from_format.size() == 0)
      common.failure("No valid devices found.");

    buildTables(false, false);
  }

  private void readFdiskFromFile(Vector input)
  {
    String[] lines;

    if (input == null)
    {
      String fname = askForFile();
      if (fname == null)
        return;
      lines = Fget.readFileToArray(fname);
    }
    else
      lines = (String[]) input.toArray(new String[0]);

    luns_from_format.removeAllElements();
    for (int i = 0; i < lines.length; i++)
    {
      // Disk /dev/sda: 80.0 GB, 80026361856 bytes
      StringTokenizer st = new StringTokenizer(lines[i], " ,:");
      if (st.countTokens() < 4)
        continue;
      if (!st.nextToken().equalsIgnoreCase("disk"))
        continue;

      SdFormat sdf = new SdFormat();
      sdf.lun      = sdf.target = st.nextToken();
      sdf.slices   = st.nextToken() + " " + st.nextToken();
      if (sdf.slices.startsWith("does"))
        continue;
      luns_from_format.add(sdf);
    }

    if (luns_from_format.size() == 0)
      JOptionPane.showMessageDialog(null,
                                    "No valid devices found. Manually check output of 'fdisk -l'",
                                    "Try again",
                                    JOptionPane.ERROR_MESSAGE);

    buildTables(false, false);
  }


  /**
   * Save the created SD parameters to a file
   */
  private void saveToFile()
  {
    String fname = askForFile();
    if (fname == null)
      return;

    Fput fp = new Fput(fname);
    if (getMask().startsWith("sd="))
      fp.println("sd=default");
    for (int i = 0; i < left_table.getRowCount(); i++)
      fp.println((String) left_table.getValueAt(i, 0));
    fp.println("");
    fp.close();
  }


  /**
   * Move selected luns from the right panel to the left panel
   */
  private void moveRightToLeft()
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
  private void moveLeftToRight()
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
   * Call the format command and intercet its output.
   */
  public static Vector doFormat(boolean prtvtoc)
  {
    lines_from_format.removeAllElements();
    OS_cmd ocmd = new OS_cmd();

    ocmd.setOutputMethod(new CommandOutput()
                         {
                           public boolean newLine(String line, String type, boolean more)
                           {
                             lines_from_format.add(type + " " + line);
                             return true;
                           }
                         });


    ocmd.addText("format << EOF");
    status("Running '" + ocmd.getCmd() + "'");
    ocmd.execute();

    status("'format' complete");

    luns_from_format.removeAllElements();

    for (int i = 0; i < lines_from_format.size(); i++)
    {
      String in = (String) lines_from_format.elementAt(i);
      String type = in.substring(0, 6);
      String line = in.substring(6);
      //common.ptod("type: " + type);
      //common.ptod("line: " + line);
      processFormatLine(line, type, prtvtoc);
    }

    if (luns_from_format.size() == 0)
    {
      JOptionPane.showMessageDialog(null,
                                    "No valid devices found. Are you sure you have root access?",
                                    "Try again",
                                    JOptionPane.ERROR_MESSAGE);
    }

    return luns_from_format;
  }

  /**
   * Call the fdisk command and intercept its output.
   */
  public Vector doFdisk()
  {
    lines_from_format.removeAllElements();
    OS_cmd ocmd = new OS_cmd();

    ocmd.setOutputMethod(new CommandOutput()
                         {
                           public boolean newLine(String line, String type, boolean more)
                           {
                             lines_from_format.add(line);
                             return true;
                           }
                         });


    ocmd.addText("fdisk -l");
    status("Running '" + ocmd.getCmd() + "'");
    ocmd.execute();

    status("'fdisk' complete");

    return lines_from_format;
  }


  /**
   * Parse format output and pick update the lun names
   */
  private static void processFormatLine(String line, String type, boolean prtvtoc)
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

      if (common.onSolaris() && prtvtoc)
        getSizes(fm);
    }
  }



  private void buildTables(boolean format, boolean prtvtoc)
  {
    if (format)
      doFormat(prtvtoc);

    left_model  = new SdLeft(new Vector(64, 0));
    right_model = new SdRight(luns_from_format);

    left_table  = new JTable(left_model);
    right_table = new JTable(right_model);

    right_table.addMouseListener(new java.awt.event.MouseAdapter()
                                 {
                                   public void mouseClicked(MouseEvent e)
                                   {
                                     if (e.getClickCount() > 1)
                                       moveRightToLeft();
                                   }
                                 });
    left_table.addMouseListener(new java.awt.event.MouseAdapter()
                                {
                                  public void mouseClicked(MouseEvent e)
                                  {
                                    if (e.getClickCount() > 1)
                                      moveLeftToRight();
                                  }
                                });



    left_table.setFont(new Font("Courier New", Font.PLAIN, 12));
    right_table.setFont(new Font("Courier New", Font.PLAIN, 12));

    left_panel  = new JScrollPane(left_table);
    right_panel = new JScrollPane(right_table);

    if (bottom != null)
      getContentPane().remove(bottom);

    bottom  = new JPanel();
    bottom.setLayout(new GridBagLayout());

    /*
    int gridx,          0
    int gridy,          1
    int gridwidth,      2
    int gridheight,     3
    double weightx,     4
    double weighty,     5
    int anchor,         6
    int fill,           7
    Insets insets,      8
    int ipadx,          9
    int ipady          10
    */

    bottom.add(left_panel,  new GridBagConstraints(0, 1, 1, 1, 0.8, 1.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    bottom.add(right_panel, new GridBagConstraints(1, 1, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

    setColumnWidth(left_table);
    setColumnWidth(right_table);

    //bottom.setSize(300, 300);
    left_panel.setPreferredSize(new Dimension(300, 9300));
    left_panel.setMinimumSize(new Dimension(300, 9900));
    right_panel.setPreferredSize(new Dimension(300, 9300));
    right_panel.setMinimumSize(new Dimension(300, 9900));

    getContentPane().add(bottom, BorderLayout.CENTER);

    do_save.setEnabled(true);
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


  private static void getSizes(SdFormat fm)
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText("prtvtoc -hs /dev/rdsk/" + fm.lun + "s*");
    ocmd.setStdout();

    status("Running '" + ocmd.getCmd() + "'");

    ocmd.execute();

    String[] lines = ocmd.getStdout();
    for (int i = 0; i < lines.length; i++)
    {
      //common.ptod("lines[i]: " + lines[i]);
      StringTokenizer st = new StringTokenizer(lines[i]);
      int slice = Integer.parseInt(st.nextToken());
      st.nextToken();
      st.nextToken();
      st.nextToken();
      long sectors = Long.parseLong(st.nextToken());
      if (slice >= 16)
        continue;
      fm.sizes[slice] = sectors * 512;
    }

    status("");
  }


  /**
   * Set the minimum column width for each column
   */
  public void setColumnWidth(JTable table)
  {
    TableColumnModel model = table.getColumnModel();
    for (int i = 0; i < table.getColumnCount(); i++)
    {
      TableWidth.sizeColumn(i, table);

      if (table == right_table)
      {
        if (common.onWindows())
        {
          DefaultTableCellRenderer rend = new DefaultTableCellRenderer();
          rend.setHorizontalAlignment(SwingConstants.LEFT);
          model.getColumn(i).setCellRenderer(rend);
        }

        else if (i > 0)
        {
          FractionCellRenderer rend = new FractionCellRenderer(5, 1, SwingConstants.RIGHT);
          model.getColumn(i).setCellRenderer(rend);
        }
      }
    }
  }


  public static void main(String[] args) //throws ClassNotFoundException
  {
    /* Set the frame to the same place and size as before: */
    SdBuild frame = new SdBuild();
    frame.setSize(1200, 600);

    Utils.Message.centerscreen(frame);
    frame.setVisible(true);
  }


  private static void status(String txt)
  {
    status_bar.setText(txt);
    status_bar.paintAll(status_bar.getGraphics());
  }


  private JComboBox createComboBox()
  {
    Vector list = new Vector(8, 0);
    if (common.onLinux())
      list.add("sd=sd#,lun=$,openflags=o_direct");
    else if (common.onWindows())
      list.add("sd=sd#,lun=\\\\.\\PhysicalDrive$");
    else
      list.add("sd=sd#,lun=/dev/rdsk/$%");
    list.add("Edit file 'build_sds.txt' for more options");

    String filename = ClassPath.classPath("build_sds.txt");
    if (Fget.file_exists(filename))
    {
      Fget fg = new Fget(filename);
      String line = null;
      while ((line = fg.get()) != null)
      {
        line = line.trim();
        if (line.length() == 0)
          continue;
        if (line.startsWith("*"))
          continue;
        list.add(line);
      }
      fg.close();
    }

    JComboBox box = new JComboBox(list);
    return box;
  }


  private void replaceParms()
  {
    String msg = "This option will replace existing SD parameters. \n" +
                 "This will only be valid if all SD names are sd1 through sd-n.";
    int rc = JOptionPane.showConfirmDialog(null, msg,
                                           "Information message",
                                           JOptionPane.OK_CANCEL_OPTION);
    if (rc != 0)
      return;

    String fname = askForFile();
    if (fname == null)
      return;

    String[] lines = Fget.readFileToArray(fname);
    Fput fp = new Fput(fname);


    /* Copy until first SD: */
    int i = 0;
    for (i = 0; i < lines.length; i++)
    {
      String line = lines[i];
      if (line.startsWith("sd") && !line.startsWith("sd=default"))
        break;
      fp.println(line);
    }

    /* Delete everything until the first WD: */
    for (; i < lines.length; i++)
    {
      String line = lines[i];
      if (line.startsWith("wd"))
        break;
    }

    /* Insert new SDs: */
    //fp.println("*The following SDs replaced by the Vdbench SD parameter generation tool.");
    for (int j = 0; j < left_model.getRowCount(); j++)
      fp.println((String) left_model.getValueAt(j, 0));

    /* Copy the rest: */
    for (; i < lines.length; i++)
    {
      String line = lines[i];
      fp.println(line);
    }

    fp.close();
  }


  public void getWindowsDiskList()
  {
    String[] disks = new String[] { "empty list"};
    try
    {
      String  type   = WindowsPDH.translateFieldNameOptional("PhysicalDisk");
      disks  = WindowsPDH.getDisks(type);
      luns_from_format = new Vector(16, 0);

      for (int i = 0; i < disks.length; i++)
      {
        String[] split = disks[i].split(" +");
        SdFormat sdf   = new SdFormat();
        sdf.lun = sdf.target = split[0];

        /* Pick up drive letters: */
        if (split.length > 1)
          sdf.slices = disks[i].substring(disks[i].indexOf(" "));
        else
          sdf.slices = "No drive letters on disks";
        luns_from_format.add(sdf);
      }

      buildTables(false, false);
    }
    catch (Exception e)
    {
      common.ptod("Not sure what happened here, but if I have a list of disks this is it:");
      for (int i = 0; disks != null && i < disks.length; i++)
        common.ptod("disks: " + disks[i]);
      common.failure(e);
    }
  }
}


class SdFormat
{
  public String disk_number;
  public String lun;
  public String slices;
  public String target;
  public long[] sizes = new long[16];

}



class SortFormat implements Comparator
{

  public int compare(Object o1, Object o2)
  {
    SdFormat e1 = (SdFormat) o1;
    SdFormat e2 = (SdFormat) o2;

    if (!e1.target.equals(e2.target))
      return e1.target.compareToIgnoreCase(e2.target);

    return e1.lun.compareToIgnoreCase(e2.lun);
  }
}
