package Vdb;

/*
 *
 * Copyright (c) 2000-2008 Sun Microsystems, Inc. All Rights Reserved.
 *
 */

import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.*;

import javax.swing.*;
import javax.swing.border.*;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableColumnModel;
import javax.swing.text.*;

import Utils.*;



/**
 * Gui portion of DVPost
 */
public class PostGui extends JFrame implements ActionListener
{
  private final static String c = "Copyright (c) 2000-2008 Sun Microsystems, Inc. " +
                                  "All Rights Reserved.";

  private JSplitPane  split_panel  = new JSplitPane();

  private JPanel      left_panel   = new JPanel();
  private JScrollPane left_scroll  = new JScrollPane();
  private JScrollPane error_scroll = new JScrollPane();

  private JScrollPane right_scroll = new JScrollPane();
  private JTextArea   right_text   = new JTextArea();
  private JPanel      buttons      = new JPanel();

  private DVModel     left_model   = new DVModel();
  private JTable      left_table   = new JTable();

  private StringBuffer overview;
  private String    SELECT          = "Display selection";
  private String    RESET           = "Reset selection";
  private JButton   overview_button = new JButton("Overview");
  private JButton   close_button    = new JButton("Close");
  private JButton   reread          = new JButton("Reread block");
  private JButton   lba_button      = new JButton("Display block");
  private JButton   only_button     = new JButton(SELECT);
  private JButton   save_button     = new JButton("Save text");
  private JButton   read_text       = new JButton("Read text");
  private JButton   read_error      = new JButton("Read errorlog");
  private JTextArea error_text      = new JTextArea();
  private String    filename       = "";

  private String   last_text = null;


  public PostGui(String file_in, BadBlock[] blks, ArrayList overvw)
  {
    filename = file_in;
    setTitle("Vdbench Data Validation post processing utility for file " +
             new File(filename).getAbsolutePath());
    Dimension dim = new Dimension(1280, 768);
    setSize(dim);
    split_panel.setSize(dim);

    /* Copy the list of text that was passed: */
    overview = new StringBuffer(65536);
    for (int i = 0; i < overvw.size(); i++)
      overview.append((String) overvw.get(i) + "\n");
    right_text.setFont(new Font("Courier New", Font.PLAIN, 12));
    setAndSaveText(overview.toString());


    left_table.setModel(left_model);
    left_model.setBlocks(blks);

    /* Set the minimum width for the columns: */
    sizeColumn(0, left_table);
    sizeColumn(1, left_table);
    sizeColumn(2, left_table);
    sizeColumn(3, left_table);

    overview_button .addActionListener(this);
    close_button    .addActionListener(this);
    lba_button      .addActionListener(this);
    reread          .addActionListener(this);
    only_button     .addActionListener(this);
    save_button     .addActionListener(this);
    read_text       .addActionListener(this);
    read_error      .addActionListener(this);
    buttons.add(close_button    );
    buttons.add(overview_button );
    buttons.add(reread          );
    buttons.add(lba_button      );
    buttons.add(only_button     );
    buttons.add(save_button     );
    buttons.add(read_text       );
    buttons.add(read_error      );

    left_panel.setLayout(new GridBagLayout());
    left_panel.add(error_scroll,  new GridBagConstraints(0, 0, 1, 1, 0.5, 0.3, GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    left_panel.add(left_scroll,   new GridBagConstraints(0, 1, 1, 1, 0.5, 0.3, GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    split_panel.setLeftComponent(left_panel);
    split_panel.setRightComponent(right_scroll);

    error_text.setBackground(new Color(238, 238, 238));

    left_scroll.setViewportView(left_table);
    right_scroll.setViewportView(right_text);
    error_scroll.setViewportView(error_text);

    error_scroll.setBorder(new TitledBorder(BorderFactory.createEtchedBorder(Color.white,new Color(148, 145, 140)),
                                           "Short error text"));
    left_scroll.setBorder(new TitledBorder(BorderFactory.createEtchedBorder(Color.white,new Color(148, 145, 140)),
                                           "Failed data blocks"));

    addComponentListener(new ComponentAdapter()
                         {
                           public void componentResized(ComponentEvent e)
                           {
                             Component c = e.getComponent();
                             //common.ptod("c: " + c.getSize());
                           }

                         });

    addWindowListener(new WindowAdapter()
                      {
                        public void windowClosing(WindowEvent e)
                        {
                          System.exit(0);
                        }
                      });

    left_table.addMouseListener(new java.awt.event.MouseAdapter()
                                {
                                  public void mousePressed(MouseEvent e)
                                  {
                                    tbl_mousePressed(e);
                                  }
                                });
    left_table.getSelectionModel().addListSelectionListener(new ListSelectionListener()
                                                            {
                                                              public void valueChanged(ListSelectionEvent e)
                                                              {
                                                                row_changed();
                                                              }
                                                            });

    right_text.addMouseListener(new MouseAdapter()
                                {
                                  public void mouseClicked(MouseEvent e)
                                  {
                                    if (SwingUtilities.isLeftMouseButton(e) && (e.getClickCount() == 2))
                                    {
                                      displayOnly();
                                    }
                                  }
                                });

    /*
    * @param gridx	The initial gridx value.
    * @param gridy	The initial gridy value.
    * @param gridwidth	The initial gridwidth value.
    * @param gridheight	The initial gridheight value.
    * @param weightx	The initial weightx value.
    * @param weighty	The initial weighty value.
    * @param anchor	The initial anchor value.
    * @param fill	The initial fill value.
    * @param insets	The initial insets value.
    * @param ipadx	The initial ipadx value.
    * @param ipady	The initial ipady value.
    */

    Container cp = getContentPane();
    cp.setLayout(new GridBagLayout());
    cp.add(buttons,      new GridBagConstraints(0, 0, 1, 1, 1.0, 0.0, GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    cp.add(split_panel,  new GridBagConstraints(0, 1, 1, 1, 1.0, 1.0, GridBagConstraints.WEST, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));

    left_table.setRowSelectionInterval(0, 0);

    split_panel.setDividerLocation(0.5);

    Message.centerscreen(this);
  }

  public void setText(String text)
  {
    right_text.setText(text);
    right_text.setCaretPosition(0);
    only_button.setText(SELECT);
  }
  public void setAndSaveText(String text)
  {
    setText(text);
    last_text = text;
  }

  public void infoMsg(String text)
  {
    JOptionPane.showMessageDialog(this, text,
                                  "Information message",
                                  JOptionPane.INFORMATION_MESSAGE);
  }

  public void actionPerformed(ActionEvent e)
  {
    String cmd = e.getActionCommand();

    if (cmd.equals(overview_button.getText()))
      setAndSaveText(overview.toString());

    else if (cmd.equals(lba_button.getText()))
      displayCurrentLba();

    else if (cmd.equals(close_button.getText()))
      this.dispose();

    else if (cmd.equals(save_button.getText()))
      saveDisplayData();

    else if (cmd.equals(read_text.getText()))
      readDisplayData(null);

    else if (cmd.equals(read_error.getText()))
      readDisplayData(filename);

    else if (cmd.equals(SELECT))
      displayOnly();

    else if (cmd.equals(RESET))
      setText(last_text);

    else if (cmd.equals(reread.getText()))
      reReadCurrentLba();
  }


  private void displayOnly()
  {
    if (right_text.getSelectedText() == null)
      return;
    String selection = right_text.getSelectedText().toLowerCase();
    StringBuffer txt = new StringBuffer(65536);

    StringTokenizer st = new StringTokenizer(last_text, "\n");
    while (st.hasMoreTokens())
    {
      String token = st.nextToken();
      if (token.toLowerCase().indexOf(selection) != -1)
        txt.append(token + "\n");
    }

    setText(txt.toString());
    only_button.setText(RESET);
  }

  /**
   * Double click in table.
   */
  void tbl_mousePressed(MouseEvent e)
  {
    /* Double click gives controller detail summary, except for COL_SIZE: */
    int row = left_table.getSelectedRow();

    if (e.getClickCount() > 1)
      displayCurrentLba();
    else
      row_changed();
  }


  /**
   * New row selected
   */
  void row_changed()
  {
    int row = left_table.getSelectedRow();
    BadBlock bb = left_model.getRowAt(row);
    error_text.setText("Double click on any row to display just this block.\n" +
                       "Double click on any data on the right side " +
                       "to only display selected values.\nErrors for this block:\n" +
                       bb.getBlockStatus());
  }


  private void saveDisplayData()
  {
    String fname = askForFile(filename, "Enter file name:");
    if (fname == null)
      return;

    if (!fname.endsWith(".txt"))
      fname += ".txt";
    Fput fp = new Fput(fname);
    fp.println(right_text.getText() + "\n");
    fp.close();
  }

  private void readDisplayData(String name)
  {
    String fname = name;
    if (name == null)
    {
      fname = askForFile(filename, "Enter file name:");
      if (fname == null)
        return;
    }

    Fget fg             = new Fget(fname);
    String line         = null;
    StringBuffer buffer = new StringBuffer(32768);
    while ((line = fg.get()) != null)
      buffer.append(line + "\n");
    fg.close();

    setAndSaveText(buffer.toString());
  }

  private void reReadCurrentLba()
  {
    int row = left_table.getSelectedRow();
    BadBlock bb = left_model.getRowAt(row);

    /* Modify the block: */
    long handle = Native.openFile(bb.lun);
    if (handle < 0)
    {
      infoMsg(String.format("Lun '%s' either does not exist or you do not have read access.", bb.lun));
      return;
    }
    Native.closeFile(handle);

    Vector lines = PrintBlock.printit(bb.lun, bb.logical_lba - bb.file_start_lba,
                                      bb.xfersize, bb.xfersize);
    StringBuffer txt = new StringBuffer(65536);
    for (int i = 0; i < lines.size(); i++)
      txt.append((String) lines.elementAt(i) + "\n");

    setAndSaveText(txt.toString());
  }

  private void displayCurrentLba()
  {
    int row = left_table.getSelectedRow();

    BadBlock bb = left_model.getRowAt(row);
    StringBuffer txt = new StringBuffer(65536);
    for (int i = 0; i < bb.raw_input.size(); i++)
      txt.append((String) bb.raw_input.get(i) + "\n");

    setAndSaveText(txt.toString());
  }


  /**
   * Automatically size the columns to the largest element.
   */
  public void sizeColumn(int col, JTable tbl)
  {
    int largest = 0;

    /* Loop thru rows: */
    for (int i = 0; i < left_model.getRowCount(); ++i)
    {
      String str = (String) left_model.getValueAt(i, col);

      /* Only store the largest size for tbl: */
      if (str != null)
        largest = Math.max(largest, tbl.getFontMetrics(tbl.getFont()).stringWidth(str));
    }

    if (tbl.getRowCount() == 0)
      return;
    Insets insets = ((JComponent)tbl.getCellRenderer (0, col)).getInsets();

    largest += insets.left + insets.right + 5;
    largest  = Math.max(largest, 25);

    tbl.getColumnModel().getColumn(col).setMinWidth(largest);
    //common.ptod("size_column(): " + col + " " + largest + " " + insets.left + " " + insets.right);
  }



  /**
   * Ask user for a file name to be used
   */
  public static String askForFile(String dir, String title)
  {
    JFileChooser fc = new JFileChooser((new File (dir).getAbsolutePath()));
    fc.setFileSelectionMode(JFileChooser.FILES_ONLY );
    fc.setDialogTitle(title);

    if (fc.showOpenDialog(null) == fc.APPROVE_OPTION)
    {
      String file = fc.getSelectedFile().getAbsolutePath();
      return file;
    }

    return null;
  }



}

class DVModel extends AbstractTableModel
{
  private String[] columns = {"Lun", "lba", "key", "Errors"};
  private BadBlock[] blocks;

  public void setBlocks(BadBlock[] blks)
  {
    blocks = blks;
  }

  public BadBlock getRowAt(int row)
  {
    return blocks[row];
  }
  public Object getValueAt(int row, int col)
  {
    BadBlock bb = blocks[row];
    if (col == 0) return bb.lun;
    if (col == 1) return String.format("0x%x", bb.logical_lba);
    if (col == 2) return String.format("0x%02x", bb.key_wanted);
    else  return bb.getBlockStatusShort();
  }
  public int getColumnCount()
  {
    return columns.length;
  }
  public int getRowCount()
  {
    return blocks.length;
  }
  public String getColumnName(int col)
  {
    return columns[col];
  }
}
