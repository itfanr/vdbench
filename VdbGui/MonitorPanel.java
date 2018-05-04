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
 * <p>Title: MonitorPanel.java</p>
 * <p>Description: This class contains the montoring subpanels for
 * the VDBench GUI.</p>
 * @author Jeff Shafer
 * @version 1.0
 */
import javax.swing.*;
import javax.swing.event.*;
import javax.swing.border.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.table.*;
import java.util.*;

public class MonitorPanel extends JPanel implements ListSelectionListener
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // A tabbed pane to hold subpanels for display of devices, workloads, and
  // execution output.
  private JTabbedPane tabbedPane = new JTabbedPane(JTabbedPane.TOP);

  // Table models corresponding to tables to display chosen devices and workloads.
  private DefaultTableModel storageListModel  = new DefaultTableModel();
  private DefaultTableModel workloadListModel = new DefaultTableModel();
  private DefaultTableModel outputListModel   = new DefaultTableModel();

  // Tables to display chosen devices and workloads.
  private EntryTable storageList  = new EntryTable(10,1);
  private EntryTable workloadList = new EntryTable(10,1);
  private JTextArea outputArea    = new JTextArea();

  // Scrollpanes containing the above tables to allow the user to scroll and view
  // all chosen entries.
  private JScrollPane storageScrollPane  = new JScrollPane(storageList);
  private JScrollPane workloadScrollPane = new JScrollPane(workloadList);
  private JScrollPane outputScrollPane   = new JScrollPane(outputArea);

  /**
   * No-argument constructor.
   */
  public MonitorPanel()
  {
    // Set the border and layout.  We use grid layout
    // to ensure that the elements fill the panel.
    setBorder(new TitledBorder("Monitor"));
    setLayout(new GridLayout(1, 1));

    // Ensure that default column headers are not visible.
    storageListModel.addColumn("");
    workloadListModel.addColumn("");
    outputListModel.addColumn("");

    // Set the models, and make sure that the selection color is the
    // same as the rest of the background so that we don't get any
    // weird color changes should the user decide to delete any entry
    // from the display panel.
    storageList.setModel(storageListModel);
    storageList.setSelectionBackground(Color.white);
    ListSelectionModel storageSelectionModel = storageList.getSelectionModel();
    workloadList.setModel(workloadListModel);
    workloadList.setSelectionBackground(Color.white);
    ListSelectionModel workloadSelectionModel = workloadList.getSelectionModel();

    // Add the mouse listener to provide the user with entry removal functionality.
    MouseClickRemover mouseRemover = new MouseClickRemover();
    workloadList.addMouseListener(mouseRemover);
    storageList.addMouseListener(mouseRemover);

    // Set fonts to fixed-width.
    storageList.setFont(new Font("Courier", Font.PLAIN, 12));
    workloadList.setFont(new Font("Courier", Font.PLAIN, 12));
    outputArea.setFont(new Font("Courier", Font.PLAIN, 12));

    // Add a list selection listener to allow parameter panel entries to be
    // updated from the lists.
    workloadSelectionModel.addListSelectionListener(this);
    storageSelectionModel.addListSelectionListener(this);

    // Turn off the grids.
    storageList.setShowGrid(false);
    workloadList.setShowGrid(false);

    // Prevent the text area's output from being modified by the user.
    outputArea.setEditable(false);

    // Add each element to the tabbed pane, and add the tabbed pane to the panel.
    tabbedPane.add("Storage Device(s)/File(s)", storageScrollPane);
    tabbedPane.add("Workload/Run Definition(s)", workloadScrollPane);
    tabbedPane.add("Execution Output", outputScrollPane);
    add(tabbedPane);
  }

  /**
   * Allows the MonitorPanel to provide a reference to the list of storage devices.
   * @return a reference to an EntryTable object.
   */
  public EntryTable getStorageList()
  {
    return storageList;
  }

  /**
   * Allows the MonitorPanel to provide a reference to the list of workload entries.
   * @return a reference to an EntryTable object.
   */
  public EntryTable getWorkloadList()
  {
    return workloadList;
  }

  /**
   * Allows the monitorPanel to provide a reference to the text area used to
   * display execution output.
   * @return a reference to the JTextArea used to display output.
   */
  public JTextArea getTextArea()
  {
    return outputArea;
  }

  /**
   * Makes the workload pane of the monitor panel visible.
   */
  public void setWorkloadPaneVisible()
  {
    tabbedPane.setSelectedIndex(1);
  }

  /**
   * Makes the file/device pane of the monitor panel visible.
   */
  public void setDevicePaneVisible()
  {
    tabbedPane.setSelectedIndex(0);
  }

  /**
   * Makes the output pane of the monitor panel visible.
   */
  public void setOutputPaneVisible()
  {
    tabbedPane.setSelectedIndex(2);
  }

  // Implementation of method valueChanged of interface ListSelectionListener.
  public void valueChanged(ListSelectionEvent e)
  {
    // See if this is a valid table selection
    if(e.getSource() == workloadList.getSelectionModel() && e.getFirstIndex() >= 0)
    {
      // Get the data model for this table
      TableModel model = (TableModel)workloadList.getModel();

      // Determine the selected item
      int selectedRow = workloadList.getSelectedRow();

      // If the selected row index is valid...
      if(selectedRow >= 0 && selectedRow < workloadList.getRowCount()/2)
      {
        //...display a warning dialog to make sure he is aware he is about to delete an entry.
        //if(warnUserBeforeDeletion("workload entry?") == JOptionPane.YES_OPTION)
        {
          //workloadListModel.removeRow(selectedRow);
          String workloadString = (String)workloadListModel.getValueAt(selectedRow, 0);
          VDBenchGUI.getWorkloadParameterPanel().setFields(workloadString, selectedRow);
        }
      }
      if(selectedRow >= workloadList.getRowCount()/2)
      {
        JOptionPane.showMessageDialog(VDBenchGUI.getGUIFrame(), "Please left click on a workload definition only.", "Click Region Error", JOptionPane.ERROR_MESSAGE);
      }
    }

    // See if this is a valid table selection
    /*if(e.getSource() == storageList.getSelectionModel() && e.getFirstIndex() >= 0)
    {
      // Get the data model for this table
      TableModel model = (TableModel)storageList.getModel();

      // Determine the selected item
      int selectedRow = storageList.getSelectedRow();

      // If the selected row index is valid...
      if(selectedRow >= 0)
      {
        //...display a warning dialog to make sure he is aware he is about to delete an entry.
        if(warnUserBeforeDeletion("device/file entry?") == JOptionPane.YES_OPTION)
        {
          storageListModel.removeRow(selectedRow);
        }
      }
    }*/
  }

  // Ask the user if he is sure he wants to delete a workload or device/file
  // entry from the display panel.
  private int warnUserBeforeDeletion(String entryType)
  {
    return JOptionPane.showConfirmDialog(this, "Do you wish to delete this " + entryType,
                                  "Delete Entry Confirmation", JOptionPane.YES_NO_OPTION, JOptionPane.WARNING_MESSAGE);
  }

  // Method to add a storage entry to the storage list.
  public void addDeviceEntry(String entry)
  {
    Object[] rowData = new Object[1];
    rowData[0] = entry;
    storageListModel.addRow(rowData);
  }

  // Method to add a workload entry to the workload list.
  public void addWorkloadEntry(String entry)
  {
    Object[] rowData = new Object[1];
    rowData[0] = entry;
    workloadListModel.addRow(rowData);
  }

  /**
   * Remove previous entries from storage device panel and workload panel.
   */
  public void clearPanels()
  {
    clearStoragePanel();
    clearWorkloadPanel();
  }

  /**
   * Removes previous entries from the storage panel.
   */
  public void clearStoragePanel()
  {
    // Get the number of entries, and remove each one.
    // Note that the actual removal call involves row zero
    // each time.  This is because each removal renumbers the
    // rows, and therefore, to remove them properly, we must
    // remove the top one (row zero) each time.
    int storageCount = storageListModel.getRowCount();
    if(storageCount > 0)
    {
      for (int i = 0; i < storageCount; i++)
      {
        storageListModel.removeRow(0);
      }
    }
  }

  // Writes the entries to the monitor panel.
  public void writeEntriesToPanel(Vector workloads, Vector runs)
  {
    // Clear the previous entries from the panel.
    clearWorkloadPanel();

    // Add the workloads to the monitor panel.
    for(int i = 0; i < workloads.size(); i++)
    {
      addWorkloadEntry((String)workloads.get(i));
    }

    // Write a line to separate the workload defs from the run defs
    // Add the workloads to the monitor panel.
    addWorkloadEntry("");

    // Add the corresponding runs to the monitor panel.
    for(int i = 0; i < runs.size(); i++)
    {
      addWorkloadEntry((String)runs.get(i));
    }
  }


  /**
   * Removes previous entries from the workload panel.
   */
  public void clearWorkloadPanel()
  {
    // Get the number of entries, and remove each one.
    // Note that the actual removal call involves row zero
    // each time.  This is because each removal renumbers the
    // rows, and therefore, to remove them properly, we must
    // remove the top one (row zero) each time.
    int workloadCount = workloadListModel.getRowCount();
    if(workloadCount > 0)
    {
      for (int i = 0; i < workloadCount; i++)
      {
        workloadListModel.removeRow(0);
      }
    }
  }

  // A private class to facilitate the removal of workload or file/device
  // entries at the user's discretion.
  private class MouseClickRemover extends MouseAdapter
  {
    public void mousePressed(MouseEvent e)
    {
      // Respond to a right click only.
      if(e.isMetaDown())
      {
        // If the user clicks on the workload list...
        if(e.getSource() == workloadList)
        {
          Vector workloads = VDBenchGUI.getWorkloadDefinitions();
          Vector runs      = VDBenchGUI.getRunDefinitions();

          // If the user clicks on a run definition, tell him to click on a workload definition instead.
          // Since RDs are dependent upon WDs, it seems appropriate to preserve that dependence here.
          if(workloadList.rowAtPoint(e.getPoint()) >= workloads.size())
          {
            JOptionPane.showMessageDialog(VDBenchGUI.getGUIFrame(), "Please right click on a workload definition only.", "Click Region Error", JOptionPane.ERROR_MESSAGE);
            return;
          }

          // Ask the user if he's sure he wants to delete the entry.
          if(warnUserBeforeDeletion("workload entry?") == JOptionPane.YES_OPTION)
          {
            // If so, get the row index.
            int index = workloadList.rowAtPoint(e.getPoint());

            // Check to make sure we have an entry, and remove it.
            if(workloads.size() > 0 && index >= 0)
            {
              workloads.remove(index);
              runs.remove(index);
            }
            // If we have entries left, refresh the panel.
            if(workloads.size() > 0)
            {
              writeEntriesToPanel(workloads, runs);
            }
            // Otherwise, just clear it.
            else
            {
              clearWorkloadPanel();
            }
          }
        }
        // If the user clicks on the device/file list...
        if(e.getSource() == storageList)
        {
          if (warnUserBeforeDeletion("device/file entry?") == JOptionPane.YES_OPTION)
          {
            storageListModel.removeRow(storageList.rowAtPoint(e.getPoint()));
          }
        }
      }
    }
  }
}
