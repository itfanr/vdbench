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
 * <p>Title: DeviceDisplayPanel.java</p>
 * <p>Description: This class creates a panel which displays storage devices
 * available on the user's system, and allows him to select those devices on
 * which he wishes to run VDBench.</p>
 * @author Jeff Shafer
 * @version 1.0
 */

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.io.*;
import java.util.*;

public class DeviceDisplayPanel extends JFrame
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // Keeps track of the number of devices chosen so that each SD has a unique
  // identifier.
  private static int entryCount = 1;

  // The main display panel, and the subpanel for the buttons.
  private JPanel displayPanel;
  private JPanel controlPanel;

  // A scroll pane to hold the display text area.
  private JScrollPane scrollPane;

  // Main display area.
  private JList displayList;

  // Field to display selected device.
  private JTextField deviceNameField;

  // Buttons to control selection and cancellation.
  private JButton selectButton;
  private JButton cancelButton;

  // A vector to hold the names of storage devices.  This is an argument to
  // the JList used to display these names.
  private Vector deviceNames;

  // A reference to this object, used to call it from the cancel button listener.
  private DeviceDisplayPanel app;

  // A dialog to warn the user about overwriting his drives.
  private AboutDialog warningPanel;


  private String none_found =
  "No raw devices were found. \n" +
  " \n" +
  "Vdbench searched for: \n" +
  "Windows: drives a-z, excluding 'c' \n" +
  "Solaris: /dev/rdsk \n" +
  "HP/UX: /dev/rdsk \n" +
  "Linux: /dev/sd or /dev/hd \n" +
  "MAC: /dev/rdisk \n" +
  "AIX: /dev/rhdisk \n";

  /**
   * Creates the panel and makes it visible.  If the argument is non-null, this panel
   * is centered over the parent.
   * @param parent a reference to a JFrame on behalf of which this panel exists.
   */
  public DeviceDisplayPanel(JFrame parent)
  {
    // Set the frame title and build its contents.
    super("Storage Device Selector");
    buildDisplayPanel();
    getContentPane().add(displayPanel);

    // Set a reference to this object so that we may close this frame when
    // the cancel button is pressed.
    app = this;

    // Size the frame.
    setSize(450, 250);

    // If a parent exists, set the dialog inside it.
    if (parent != null)
    {
      Dimension parentDimension = parent.getSize();
      Point p = parent.getLocation();
      setLocation(p.x + parentDimension.width/4, p.y + parentDimension.height/4);
    }
    setResizable(true);

    // Create the warning panel to advise the user of the dangers of writing to disk.
    String [] info = {"Workloads involving writes to disk can corrupt data!  Bear this in mind when running on data storage devices!"};
    warningPanel = new AboutDialog(parent, "Potential Data Corruption Warning", info);
    warningPanel.addCancelButtonAndListener(new WarningCancelButtonListener());
  }

  /**
   * Shows a warning panel, and then makes the device display panel visible.
   */
  public void makeVisible()
  {
    // Refresh the listing every time.
    if (obtainAndDisplayDevices())
      setVisible(true);
  }

  // Build the various panel components and add them to the panel.
  private void buildDisplayPanel()
  {
    // Create the main panel.
    displayPanel = new JPanel();
    displayPanel.setLayout(new BorderLayout());

    // Create components for the control panel.
    JLabel deviceNameLabel = new JLabel("Device Name");
    deviceNameField = new JTextField(10);
    deviceNameField.setEditable(false);
    selectButton = new JButton("Select");
    cancelButton = new JButton("Cancel");

    // Add the button listeners.
    selectButton.addActionListener(new SelectButtonListener());
    cancelButton.addActionListener(new CancelButtonListener());

    // Create the control panel, and set the horizontal gap between elements to
    // an acceptable value.
    controlPanel = new JPanel();
    ((FlowLayout)controlPanel.getLayout()).setHgap(12);
    controlPanel.add(deviceNameLabel);
    controlPanel.add(deviceNameField);
    controlPanel.add(selectButton);
    controlPanel.add(cancelButton);

    // Create the device name vector and populate it.
    obtainAndDisplayDevices();

    // Add the display area to the main panel.  Note that only single
    // selection is allowed.
    displayList = new JList(deviceNames);
    displayList.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    scrollPane = new JScrollPane(displayList);
    displayPanel.add(scrollPane, BorderLayout.CENTER);
    displayPanel.add(controlPanel, BorderLayout.SOUTH);

    // Add a mouse listener to the display list's listener list.
    displayList.addMouseListener(new ListMouseAdapter());

    // Add additional empty panels to properly frame the text area.
    displayPanel.add(new JPanel(), BorderLayout.NORTH);
    displayPanel.add(new JPanel(), BorderLayout.WEST);
    displayPanel.add(new JPanel(), BorderLayout.EAST);
  }

  // Get the list of storage devices from the operating system, and
  // display them in the text area.
  private boolean obtainAndDisplayDevices()
  {
    deviceNames = new Vector();

    int os = OperatingSystemIdentifier.determineOperatingSystem();
    switch (os)
    {
      case(OperatingSystemIdentifier.WINDOWS):
        {
          // In windows, drives names are letters or numbers.
          // Note that we have removed "c" so that it is not possible
          // for the user to overwrite his operating system.
          String drives = "abdefghijklmnopqrstuvwxyz0123456789";
          //String drives = "ab";

          for (int i = 0; i < drives.length(); i++)
          {
            // Create a file object corresponding to each element of the above string.
            File drive = new File(drives.substring(i,i+1) + ":");
            if (drive.exists())
            {
              // If there's a file object corresponding to a drive, the drive is present, so
              // add it to the list.
              deviceNames.add(drives.substring(i, i + 1) + ":");
            }
          }

          // Sort the device names so they appear in an ordered fashion.
          Collections.sort(deviceNames);
          break;
        }


      case(OperatingSystemIdentifier.SOLARIS):
        {
          // Get a list of all the file objects in "/dev/rdsk"
          File deviceDirectory = new File("/dev/rdsk");
          File[] devices = deviceDirectory.listFiles();

          for (int i = 0; i < devices.length; i++)
          {
            deviceNames.add(devices[i].getAbsolutePath());
          }

          // Sort the device names so they appear in an ordered fashion.
          Collections.sort(deviceNames);

          break;
        }


      case(OperatingSystemIdentifier.HP):
        {
          // Get a list of all the file objects in "/dev/rdsk"
          File deviceDirectory = new File("/dev/rdsk");
          File[] devices = deviceDirectory.listFiles();

          for (int i = 0; i < devices.length; i++)
          {
            deviceNames.add(devices[i].getAbsolutePath());
          }

          // Sort the device names so they appear in an ordered fashion.
          Collections.sort(deviceNames);

          break;
        }


      case(OperatingSystemIdentifier.MAC):
        {
          // Get a list of all the file objects in "/dev/rdsk"
          File deviceDirectory = new File("/dev");
          File[] devices = deviceDirectory.listFiles();

          for (int i = 0; i < devices.length; i++)
          {
            String name = devices[i].getAbsolutePath();
            if (name.startsWith("/dev/rdisk"))
              deviceNames.add(devices[i].getAbsolutePath());
          }

          // Sort the device names so they appear in an ordered fashion.
          Collections.sort(deviceNames);

          break;
        }


      case(OperatingSystemIdentifier.LINUX):
        {
          // Get a list of all the file objects in "/dev/rdsk"
          File deviceDirectory = new File("/dev");
          File[] devices = deviceDirectory.listFiles();

          for (int i = 0; i < devices.length; i++)
          {
            String name = devices[i].getAbsolutePath();
            if (name.startsWith("/dev/sd") || name.startsWith("/dev/hd"))
              deviceNames.add(devices[i].getAbsolutePath());
          }

          // Sort the device names so they appear in an ordered fashion.
          Collections.sort(deviceNames);

          break;
        }


      case(OperatingSystemIdentifier.AIX):
        {
          // Get a list of all the file objects in "/dev/rdsk"
          File deviceDirectory = new File("/dev");
          File[] devices = deviceDirectory.listFiles();

          for (int i = 0; i < devices.length; i++)
          {
            String name = devices[i].getAbsolutePath();
            if (name.startsWith("/dev/rhdisk"))
              deviceNames.add(devices[i].getAbsolutePath());
          }

          // Sort the device names so they appear in an ordered fashion.
          Collections.sort(deviceNames);

          break;
        }

      default:
        {
          System.out.println("Unknown");
        }
    }

    if (deviceNames.size() == 0)
    {
      JOptionPane.showMessageDialog(null, none_found, "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
      return false;
    }

    return true;
  }

  // Formats the lun name in the case of a raw mounted Windows disk.
  // For example, "C:\" becomes "\\.\C:".
  private String formatLUN(String lun)
  {
    String formattedLun = "";
    String windowsFormatPrefix = "\\\\.\\";

    int os = OperatingSystemIdentifier.determineOperatingSystem();

    if (os == OperatingSystemIdentifier.WINDOWS)
    {
      // Strip off the terminating "\", if present.
      if (lun.endsWith("\\"))
      {
        lun = lun.substring(0, lun.length()-1);
      }

      // Add "\\.\" to the front of the string.
      formattedLun = windowsFormatPrefix + lun;
    }

    else
    //if(os == OperatingSystemIdentifier.SOLARIS)
    {
      formattedLun = lun;
    }
    return formattedLun;
  }

  // Listen to the selection button.
  private class SelectButtonListener implements ActionListener
  {
    public void actionPerformed(ActionEvent e)
    {
      // Get the selected device from the text field.
      String selectedDevice = deviceNameField.getText();

      // If the user hits "Select" without making a choice, let him know.
      if (selectedDevice.equalsIgnoreCase(""))
      {
        JOptionPane.showMessageDialog(app, "Please select a device.", "Invalid Device Selection", JOptionPane.ERROR_MESSAGE);
        return;
      }

      (VDBenchGUI.getMonitorPanel()).setDevicePaneVisible();

      // Show the warning panel.
      if (!Vdb.common.get_debug(Vdb.common.NO_GUI_WARNING))
        warningPanel.setVisible(true);

      if (warningPanel.isApproved() ||
          Vdb.common.get_debug(Vdb.common.NO_GUI_WARNING))
      {
        // Properly format the selected device string, and place the appropriate entry string
        // in the monitor panel.
        String lun = formatLUN(selectedDevice);
        String entryString = "sd=sd" + ButtonPanel.getEntryCount() + ",lun=" + lun;

        (VDBenchGUI.getMonitorPanel()).addDeviceEntry(entryString);
        warningPanel.setDisapproval();
      }

      // Finally, since we are done with this window, make it invisible.
      app.setVisible(false);

      // And, clear the field and previous selection, so it's fresh the next
      // time it is called up.
      deviceNameField.setText("");
      displayList.clearSelection();
    }
  }

  // Listen to the cancel button.
  private class CancelButtonListener implements ActionListener
  {
    public void actionPerformed(ActionEvent e)
    {
      // Dispose of the application since it is no longer needed.
      app.setVisible(false);
      deviceNameField.setText("");
      displayList.clearSelection();
    }
  }

  // Listenes to the cancel button of the associated warning panel.
  private class WarningCancelButtonListener implements ActionListener
  {
    public void actionPerformed(ActionEvent e)
    {
      warningPanel.setDisapproval();
      warningPanel.setVisible(false);
    }
  }

  // When the user clicks on an entry, add it to the device name field.
  private class ListMouseAdapter extends MouseAdapter
  {
    public void mouseClicked(MouseEvent e)
    {
      // If the user clicks on an entry, place that entry in the device
      // name field.
      if (e.getClickCount() == 1)
      {
        deviceNameField.setText((String)(displayList.getSelectedValue()));
      }
    }
  }

  // A short main to test.
  public static void main(String [] args)
  {
    DeviceDisplayPanel app = new DeviceDisplayPanel(null);
  }
}
