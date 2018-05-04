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
import Utils.common;
import Utils.Fget;
import Utils.Fput;

/**
 * Vdbench workload comparator.
 *
 * It obtains a set of 'old' and 'new' directories and reads all summary.html
 * and flatfile.html files. It compares several numbers and displays
 * them and the delta percentages in a JTable.
 * delta percentages are colior coded: gree is good, red is bad.
 *
 */
public class WlComp extends JFrame implements ActionListener
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  public static String old_dir = null;
  public static String new_dir = null;

  public static WlComp wlcomp = null;

  public Vector old_runs      = null;
  public Vector new_runs      = null;
  public HashMap all_keywords = null;
  public DataModel dm;

  private static JPanel  top_panel  = null;
  private static JButton old_button = new JButton("Old directory");
  private static JButton new_button = new JButton("New directory");
  private static JButton cmp_button = new JButton("Compare");
  private static JButton ext_button = new JButton("Exit");

  public static JScrollPane table_panel = null;

  public WlComp()
  {
    wlcomp = this;
    addWindowListener(new WindowAdapter()
                      {
                        public void windowClosing(WindowEvent e)
                        {
                          StoredParms.storeParms();
                          System.exit(0);
                        }
                      });

    setJMenuBar(new WlMenus(this));

    setCompareTitle();

    old_button.addActionListener(this);
    new_button.addActionListener(this);
    cmp_button.addActionListener(this);
    ext_button.addActionListener(this);

    buildTopPanel();
  }




  public void buildTopPanel()
  {
    if (top_panel != null)
      getContentPane().remove(top_panel);

    int x = 0;
    top_panel = new JPanel();
    top_panel.setLayout(new GridBagLayout());
    top_panel.add(old_button, new GridBagConstraints(x++, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    top_panel.add(new_button, new GridBagConstraints(x++, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    top_panel.add(cmp_button, new GridBagConstraints(x++, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));
    top_panel.add(ext_button, new GridBagConstraints(x++, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

    Delta[] deltas = Delta.getDeltas();
    for (int i = 0; i < deltas.length; i++)
      top_panel.add(deltas[i], new GridBagConstraints(x++, 0, 1, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

    getContentPane().add(top_panel, BorderLayout.NORTH);
  }


  public static void main(String[] args) //throws ClassNotFoundException
  {
    /* Read saved parameters: */
    StoredParms.loadParms();

    /* Set the frame to the same place and size as before: */
    WlComp frame = new WlComp();
    frame.setSize(StoredParms.last_width, StoredParms.last_height);
    frame.setLocation(StoredParms.last_x, StoredParms.last_y);
    frame.setVisible(true);
  }


  public void doCompare()
  {
    clearTable();

    try
    {
      loadAllData();

      //parseDescriptions();

      dm = new DataModel(this);
      JTable table = new JTable(dm);
      dm.setRenderers(table);
      dm.setColumnWidth(table);

      table_panel = new JScrollPane(table);

      getContentPane().add(table_panel, BorderLayout.CENTER);
      setVisible(true);

    }

    catch (CompException e)
    {
      JOptionPane.showMessageDialog(this, "Error during Workload Compare: \n" +
                                    e.getMessage(),
                                    "Processing Aborted",
                                    JOptionPane.ERROR_MESSAGE);
    }
    catch (Exception e)
    {
      StackTraceElement[] stack = e.getStackTrace();
      String txt = "\nStack Trace: ";
      for (int i = 0; stack != null && i < stack.length; i++)
        txt += "\n at " + stack[i].toString();

      JOptionPane.showMessageDialog(this, "Error during Workload Compare: \n" +
                                    e.toString() + "\t" +
                                    txt,
                                    "Processing Aborted",
                                    JOptionPane.ERROR_MESSAGE);
    }
  }


  /**
   * Load all data belonging to both directories
   */
  private void loadAllData()
  {
    Vector old_files = new Vector(8, 0);
    Vector new_files = new Vector(8, 0);
    Vector old_flats = new Vector(8, 0);
    Vector new_flats = new Vector(8, 0);

    listDir(old_dir, "old", old_files);
    listDir(new_dir, "new", new_files);

    if (old_files.size() != new_files.size())
      throw new CompException("'old' and 'new' must have the same amount of " +
                              "'flatfile.html' files in its directory structure: " +
                              old_files.size() + " vs. " + new_files.size());

    /* Load all run data: */
    old_runs = new Vector(8, 0);
    for (int i = 0; i < old_files.size(); i++)
    {
      FlatFile ff = new FlatFile((String) old_files.elementAt(i), old_dir);
      old_flats.add(ff);
      ParseFlat.parseFlatFile(ff, old_runs);
    }

    new_runs = new Vector (8, 0);
    for (int i = 0; i < new_files.size(); i++)
    {
      FlatFile ff = new FlatFile((String) new_files.elementAt(i), new_dir);
      new_flats.add(ff);
      ParseFlat.parseFlatFile(ff, new_runs);
    }

    /* Pick up summary.html run descriptions if needed: */
    ParseFlat.parseSummary(old_flats);
    ParseFlat.parseSummary(new_flats);

    parseDescriptions();

    if (old_runs.size() != new_runs.size())
    {
      String txt = String.format("On the left size we have %d runs, while on the "+
                                 "right side we have only %d. \nContinue after removing the extra runs?",
                                 old_runs.size(), new_runs.size());
      if (!askOkCancel(txt))
      {
        ShowMissingRuns(old_runs, new_runs,
                        "<html>'old' and 'new' must have the same amount of " +
                        "<br>'flatfile.html' files in its directory structure " +
                        "<br>with the same amount of complete runs: " +
                        old_runs.size() + " vs. " + new_runs.size());
        throw new CompException("Please specify a different directory name");
      }
      else
        removeLastRuns();
    }

    if (old_runs.size() == 0)
      throw new CompException("No valid runs found in flatfile.html");
  }


  private void removeLastRuns()
  {
    while (old_runs.size() > new_runs.size())
      old_runs.remove(old_runs.lastElement());

    while (old_runs.size() < new_runs.size())
      new_runs.remove(new_runs.lastElement());
  }

  /**
   * Parse run descriptions for each run.
   * Return a list of unique forxx values.
   */
  private HashMap parseDescriptions()
  {
    all_keywords = new HashMap();
    for (int i = 0; i < old_runs.size(); i++)
    {
      Run run = (Run) old_runs.elementAt(i);
      run.parseForxxValues(all_keywords);
    }

    for (int i = 0; i < new_runs.size(); i++)
    {
      Run run = (Run) new_runs.elementAt(i);
      run.parseForxxValues(all_keywords);
    }

    return all_keywords;
  }


  /**
   * List the requested directory, looking for 'flatfile.html'
   */
  private void listDir(String dir, String label, Vector file_list)
  {
    if (dir == null)
      throw new CompException("No '" + label + "' directory name specified");

    /* If this directory name is in the format of 'hnn' for h1/../h8, etc */
    /* it must be ignored. This is the (empty) flatfile directory that is */
    /* created BEFORE Vdbench 5.00 by each slave JVM.                     */
    String parent = new File(dir).getName();
    if (parent.startsWith("h"))
    {
      try
      {
        if (Integer.parseInt(parent.substring(1)) <= 100)
          return;
      }
      catch (NumberFormatException e)
      {
      }
    }

    File file = new File(dir);
    if (!file.exists())
      throw new CompException("'" + label + "' directory does not exist: " + dir);
    if (!file.isDirectory())
      throw new CompException("'" + label + "' directory name is a file name: " + dir);

    String[] files = file.list();
    for (int i = 0; i < files.length; i++)
    {
      if (files[i].equalsIgnoreCase("flatfile.html"))
        file_list.add(dir + File.separator + files[i]);

      else if (new File(dir, files[i]).isDirectory())
        listDir(dir + File.separator + files[i], label, file_list);
    }
  }


  private String getDirectory(String dir, String label)
  {
    JFileChooser fc = new JFileChooser();
    fc.setDialogTitle("Select '" + label + "' Directory");
    fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

    if (dir != null && Fget.dir_exists(dir))
      fc.setCurrentDirectory(new File(dir).getParentFile());

    /* Keep the same old directory? */
    if (fc.showOpenDialog(null) != fc.APPROVE_OPTION)
      return dir;

    /* Get new directory: */
    dir = fc.getSelectedFile().getAbsolutePath();

    if (table_panel != null)
    {
      getContentPane().remove(table_panel);
      update(getGraphics());
      setVisible(true);
    }

    return dir;
  }

  public String getFile()
  {
    JFileChooser fc = new JFileChooser();
    fc.setDialogTitle("Select file");
    fc.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);

    /* Keep the same old directory? */
    if (fc.showOpenDialog(null) != fc.APPROVE_OPTION)
      return null;

    /* Get new file: */
    return fc.getSelectedFile().getAbsolutePath();
  }

  public void actionPerformed(ActionEvent e)
  {
    String cmd = e.getActionCommand();

    if (cmd.equals(old_button.getText()))
    {
      old_dir = getDirectory(old_dir, "old");
      clearTable();
    }

    else if (cmd.equals(new_button.getText()))
    {
      new_dir = getDirectory(new_dir, "new");
      clearTable();
    }

    else if (cmd.equals(cmp_button.getText()))
      doCompare();

    else if (cmd.equals(ext_button.getText()))
    {
      StoredParms.storeParms();
      System.exit(0);
    }

    setCompareTitle();
  }

  private void setCompareTitle()
  {
    setTitle("Vdbench Workload Comparator.     Old: " +
             ((old_dir == null) ? "n/a" : old_dir + "    -    New: ") +
             ((new_dir == null) ? "n/a" : new_dir));
  }

  public void clearTable()
  {
    if (table_panel != null)
    {
      getContentPane().remove(table_panel);
      update(getGraphics());
      setVisible(true);
      table_panel = null;
    }
  }


  /**
   * Show which files are missing
   */
  private void ShowMissingRuns(Vector old_runs, Vector new_runs, String label)
  {
    StringBuffer buf = new StringBuffer(32768);
    JDialog dialog = new JDialog();
    JScrollPane scroll = new JScrollPane();
    dialog.setModal(true);

    JTextArea text = new JTextArea();
    text.setFont(new Font("Courier New", Font.PLAIN, 12));


    /* Sort old runs: */
    String[] old_names = new String[old_runs.size()];
    for (int i = 0; i < old_runs.size(); i++)
    {
      Run run = (Run) old_runs.elementAt(i);
      old_names[i] = run.flatfile_name.substring(run.base_dir.length() + 1) +
                     " rd=" + run.rd_name + getForValues(run);
      buf.append("old: " + old_names[i] + "\n");
    }
    Arrays.sort(old_names);


    /* Sort new runs: */
    String[] new_names = new String[new_runs.size()];
    for (int i = 0; i < new_runs.size(); i++)
    {
      Run run = (Run) new_runs.elementAt(i);
      new_names[i] = run.flatfile_name.substring(run.base_dir.length() + 1) +
                     " rd=" + run.rd_name + getForValues(run);
      buf.append("new: " + new_names[i] + "\n");
    }
    Arrays.sort(new_names);

    int oldx = 0;
    int newx = 0;
    while (oldx < old_names.length && newx < new_names.length )
    {
      int rc = old_names[oldx].compareTo(new_names[newx]);
      if (rc == 0)
      {
        oldx++;
        newx++;
      }

      else if (rc < 0)
        buf.append(" Missing on 'new': " + old_names[oldx++] + "\n");

      else
        buf.append(" Missing on 'old': " + new_names[newx++] + "\n");
    }


    dialog.add(new JLabel(label), BorderLayout.NORTH);
    dialog.add(scroll, BorderLayout.CENTER);

    text.setText(buf.toString());
    scroll.getViewport().add(text);

    dialog.setSize(700,400);
    Utils.Message.centerscreen(dialog);
    dialog.setVisible(true);
  }

  private static String getForValues(Run run)
  {
    String ret = "";

    if (run.forxx_values == null)
      return "x";
    if (run.forxx_values.entrySet() == null)
      return "y";

    Iterator it = run.forxx_values.entrySet().iterator();
    while (run.forxx_values != null && it != null && it.hasNext())
    {
      Map.Entry e1 = (Map.Entry) it.next();
      ret += " for" + e1.getKey() + "=" + (int) ((Double) e1.getValue()).doubleValue();
    }

    return ret;
  }


  public boolean askOkCancel(String text)
  {
    int rc = JOptionPane.showConfirmDialog(this, text,
                                           "Information message",
                                           JOptionPane.OK_CANCEL_OPTION);

    return rc == JOptionPane.OK_OPTION;
  }


}
