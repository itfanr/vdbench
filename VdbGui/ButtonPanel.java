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
* <p>Title: ButtonPanel.java</p>
* <p>Description: This class constructs the button panel on the left side
 * of the GUI and responds to user's use of those buttons.</p>
* @author: Jeff Shafer
* @version: 1.0
*/

import javax.swing.*;
import javax.swing.filechooser.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.util.*;

public class ButtonPanel extends JPanel
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // The number of buttons on the panel.
  private final static int NUMBER_OF_BUTTONS = 5;

  // Counter to differentiate storage definition listings.
  private static int entryCount = 1;

  // Number of port to be used.
  private final static int portNumber = 10656;

  // Buttons for the panel.
  private JButton selectStorageButton = new JButton("Select Storage Device");
  private JButton selectFileButton    = new JButton("Select File(s)");
  private JButton startRunButton      = new JButton("Start Run");
  private JButton stopRunButton       = new JButton("Stop Run");
  private JButton displayChartsButton = new JButton("Display Charts");

  // Subpanel for button layout.
  private JPanel[] buttonSubpanel;

  // References to other panels so methods of this class may
  // interact with them.
  private MultiGraphPanel multiGraphPanel;
  private RunParameterPanel runParameterPanel;
  private MonitorPanel monitorPanel;
  private WorkloadParameterPanel workloadPanel;
  private DeviceDisplayPanel devicePanel;

  // Tables to hold lists of devices and workloads.
  private EntryTable deviceList;
  private EntryTable workloadList;

  // The process under which VDBench runs.
  private Process process;

  // A file object which represents the VDBench parameter file written
  // by this application.
  private File parmfile;


  private static String sep = System.getProperty("file.separator");

  /**
   * No-argument constructor.
   */
  public ButtonPanel()
  {
    // Set the layout and add the buttons.
    setLayout(new GridLayout(5, 1, 5, 5));
    addButtons();

    // Set the button tool tips.
    setToolTips();

    // Set the appropriate action listeners.
    selectStorageButton.addActionListener(new DisplayDevicesActionListener());
    selectFileButton.addActionListener(new DisplayFilesActionListener());
    startRunButton.addActionListener(new StartRunActionListener());
    stopRunButton.addActionListener(new StopRunActionListener());
    displayChartsButton.addActionListener(new DisplayChartsActionListener());
  }

  /**
   * Sets a reference to the device display panel created when the application
   * was started.  This panel was created there so that it would be ready for
   * the user when he calls for it, rather than having him wait for it to be
   * constructed.
   * @param dPanel a reference to the existing <code>DeviceDisplayPanel</code>.
   */
  public void setDeviceDisplayPanel(DeviceDisplayPanel dPanel)
  {
    devicePanel = dPanel;
  }

  /**
   * Sets a reference to the table containing workload entries.
   * @param wList a reference to the table containing chosen workloads.
   */
  public void setWorkloadList(EntryTable wList)
  {
    workloadList = wList;
  }

  /**
   * Sets a reference to the table containing device/file entries.
   * @param dList a reference to the table containing chosen storage devices/files.
   */
  public void setDeviceList(EntryTable dList)
  {
    deviceList = dList;
  }

  /**
   * Sets a reference to the panel containing run definition entries.
   * @param runPanel the panel containing run definition parameters.
   */
  public void setRunParameterPanel(RunParameterPanel runPanel)
  {
    runParameterPanel = runPanel;
  }

  /**
   * Sets a reference to the monitor panel.
   * @param monPanel the panel displaying <code>VDBench</code> parameters.
   */
  public void setMonitorPanel(MonitorPanel monPanel)
  {
    monitorPanel = monPanel;
  }

  /**
   * Sets a reference to the <code>MultiGraphPanel</code> object.
   * @param multiPanel the panel displaying <code>VDBench</code> charts.
   */
  public void setMultiGraphPanel(MultiGraphPanel multiPanel)
  {
    multiGraphPanel = multiPanel;
  }

  /**
   * Sets a reference to the <code>WorkloadParameterPanel</code> object.
   * @param workPanel the panel displaying workload parameters.
   */
  public void setWorkloadPanel(WorkloadParameterPanel workPanel)
  {
    workloadPanel = workPanel;
  }

  /**
   * Allows other classes to get the current activity panel reference.
   * @return a reference to the activity panel displaying real-time charts.
   */
  public MultiGraphPanel getActivityPanel()
  {
    return multiGraphPanel;
  }

  /**
   * Allows the "Display Charts" button to be accessed and enabled at the appropriate time.
   * @return a reference to the "Display Charts" button.
   */
  public JButton getDisplayButton()
  {
    return displayChartsButton;
  }

  /**
   * Allows access to the process running <code>VDBench</code>.
   * @return a reference to the process in which <code>VDBench</code> is running.
   */
  public Process getProcess()
  {
    return process;
  }

  /**
   * Allows access to the port number chosen for communication between
   * the main application and the gui.
   * @return the designated port number for communication.
   */
  public static int getPortNumber()
  {
    return portNumber;
  }

  public static int getEntryCount()
  {
    return entryCount++;
  }

  // Creates buttons for basic functions such as selecting storage
  // starting runs, etc., and adds them to the panel.
  private void addButtons()
  {
    buttonSubpanel = new JPanel[NUMBER_OF_BUTTONS];

    // Create subpanels to contain each button.
    for(int i = 0; i < NUMBER_OF_BUTTONS; i++)
    {
      buttonSubpanel[i] = new JPanel(new GridLayout(1, 1));
    }

    // Add the buttons to the subpanels.
    // This is done to layout the buttons with
    // FlowLayout in a nice pattern.
    buttonSubpanel[0].add(selectStorageButton);
    buttonSubpanel[1].add(selectFileButton);
    buttonSubpanel[2].add(startRunButton);
    buttonSubpanel[3].add(stopRunButton);
    //buttonSubpanel[4].add(displayChartsButton);

    // Disable the stop run button until we have a run to stop.
    stopRunButton.setEnabled(false);

    // Disable the displayChartsButton until there's something to display.
    displayChartsButton.setEnabled(false);

    // Add the subpanels to the main panel.
    for(int i = 0; i < NUMBER_OF_BUTTONS; i++)
    {
      add(buttonSubpanel[i]);
    }
  }

  // Sets tool tip messages.
  private void setToolTips()
  {
    selectStorageButton.setToolTipText("Allows selection of desired storage device.");
    selectFileButton.setToolTipText("Allows selection of desired file(s).");
    startRunButton.setToolTipText("Initiates specified VDBench runs with indicated parameters.");
    stopRunButton.setToolTipText("Stops all VDBench runs.");
    displayChartsButton.setToolTipText("Displays real-time activity charts, including I/O rate, response time, and data rate.");
  }

  // Returns the path associated with each File object.
  private String[] getFilenames(File[] files)
  {
    String [] filenames = null;
    int numFiles = files.length;

    if(files.length > 0)
    {
      filenames = new String[numFiles];

      for(int i = 0; i < numFiles; i++)
      {
        filenames[i] = files[i].getPath();
      }
    }
    return filenames;
  }

  // Enables or disables buttons.
  private void enableButtons(boolean isEnabled)
  {
    // Enable or disable the buttons we don't want the user to push while execution is ocurring.
    selectStorageButton.setEnabled(isEnabled);
    selectFileButton.setEnabled(isEnabled);
    startRunButton.setEnabled(isEnabled);
    workloadPanel.getWorkloadButton().setEnabled(isEnabled);
  }

  // An inner class to listen for the "Display Device(s) button.
  private class DisplayDevicesActionListener implements ActionListener
  {
    public void actionPerformed(ActionEvent e)
    {
      // Make the device display panel visible.
      devicePanel.makeVisible();

      /*if(filenames != null)
      {
        for (int i = 0; i < filenames.length; i++)
        {
          // Format the LUN properly.
          String lun = formatLUN(filenames[i]);

          // Creates a String with device names.
          String entryString = "sd=sd" + entryCount + ",lun=" + lun;

          // Note that entry count is incremented to ensure that each entry name is unique.
          entryCount++;

          // Add the properly formatted string to the entry panel.
          (VDBenchGUI.getMonitorPanel()).addDeviceEntry(entryString);
        }
      }*/
    }
  }

  // An inner class to listen for the "Display File(s) button.
  private class DisplayFilesActionListener implements ActionListener
  {
    public void actionPerformed(ActionEvent e)
    {
      // Instantiate a file chooser, capable of selecting files only.
      JFileChooser chooser = new JFileChooser();
      chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
      chooser.setMultiSelectionEnabled(true);
      int state = chooser.showOpenDialog(null);

      // Select the device tabbed pane, to be sure that the
      // user can see the devices as he selects them.
      monitorPanel.setDevicePaneVisible();

      if(state == JFileChooser.APPROVE_OPTION)
      {
        File [] files = chooser.getSelectedFiles();
        String [] filenames = getFilenames(files);

        if (filenames != null)
        {
          for (int i = 0; i < filenames.length; i++)
          {
            // Create a string tokenizer and count the number of tokens to determine
            // if the filename contains any white space.  If it does, enclose it in quotes.
            StringTokenizer st = new StringTokenizer(filenames[i].trim());
            if (st.countTokens() > 1)
            {
              filenames[i] = "\"" + filenames[i] + "\"";
            }

            // Creates a String with file names.
            String entryString = "sd=sd" + getEntryCount() + ",lun=" + filenames[i];

            // If the file is new (i.e., it does not already exist, set the default file size to 10 mb.
            if (!files[i].exists())
            {
              entryString += ",size=10mb";
            }

            // Add the properly formatted string to the device/file panel.
            monitorPanel.addDeviceEntry(entryString);
          }
        }
      }

      // If the user opts to do nothing, simply return.
      if(state == JFileChooser.CANCEL_OPTION)
      {
        return;
      }
    }
  }

  // An inner class to listen for the displayChartsButton.
  private class DisplayChartsActionListener implements ActionListener
  {
    public void actionPerformed(ActionEvent e)
    {
      // Make the graph panel visible.
      multiGraphPanel.setVisible(true);

      // If the frame has been iconified, make sure that
      // pressing the "Display Charts" button restores it.
      if(multiGraphPanel.getState() == Frame.ICONIFIED)
      {
        multiGraphPanel.setState(Frame.NORMAL);
      }
    }
  }

  // An inner class to listen for the startRunButton.
  private class StartRunActionListener implements ActionListener
  {
    private PrintWriter dataWriter;
    String executionString;

    public void actionPerformed(ActionEvent e)
    {
      // Check to make sure that we have devices and workloads selected
      // for this run.
      if(deviceList.getRowCount() == 0)
      {
        JOptionPane.showMessageDialog(null, "No devices or files have been selected.", "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
        return;
      }
      else if(workloadList.getRowCount() == 0)
      {
        JOptionPane.showMessageDialog(null, "No workloads have been selected.", "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
        return;
      }

      // Create parmfile to run VDBench.
      try
      {
        // Write the parameter file as a temp file.
        parmfile = File.createTempFile("parmfile", ".txt");
      }
      catch(IOException ioe)
      {
        JOptionPane.showMessageDialog(null, ioe.getMessage(), "Error Creating Parameter File", JOptionPane.ERROR_MESSAGE);
        ioe.printStackTrace();
      }

      // Now, write the file which will be run.  Note that error checking occurs in the writeFile method.
      try
      {
        VDBenchGUI.getGUIFrame().writeFile(parmfile);
      }
      catch(IOException ioe)
      {
        JOptionPane.showMessageDialog(null, ioe.getMessage(), "Error Writing Parameter File", JOptionPane.ERROR_MESSAGE);
        ioe.printStackTrace();
        return;
      }
      catch(Exception ex)
      {
        JOptionPane.showMessageDialog(null, ex.getMessage(), "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
        ex.printStackTrace();
        return;
      }

      // Disable the buttons we don't want the user to push while execution is ocurring.
      selectStorageButton.setEnabled(false);
      selectFileButton.setEnabled(false);
      startRunButton.setEnabled(false);
      stopRunButton.setEnabled(true);
      workloadPanel.getWorkloadButton().setEnabled(false);
      VDBenchGUI.getGUIFrame().disableFileMenu();
      runParameterPanel.setEditable(false);

      // If the displayChartsButton was enabled during a prior run, disable it
      // now, until we have something new to display.
      if(displayChartsButton.isEnabled())
      {
        displayChartsButton.setEnabled(false);
        //multiGraphPanel.setVisible(false);
        multiGraphPanel = new MultiGraphPanel();
        VDBenchGUI.resetMultiGraphPanel(multiGraphPanel);
      }

      try
      {
        // Create a new set of charts for this run.
        multiGraphPanel.renewCharts();

        // Start the server to get data.  This must be done before VDBench is run.
        multiGraphPanel.start();

        // Get the path to the parameter file.
       String filepath = parmfile.getAbsolutePath();
       String classpath = getClassPathDir();

       // For classpath we only need the directory where 'iogen.jar' resides.
       //classpath = classpath.substring(0, classpath.indexOf("iogen.jar"));

       // We must make sure we pick up the same java version as the gui is running from.
       String javahome = System.getProperty("java.home") + sep + "bin" + sep + "java";

       // Add quotes to make sure that directories containing blanks are handled properly.
       //if(OperatingSystemIdentifier.determineOperatingSystem() == OperatingSystemIdentifier.WINDOWS)
       //{
       //  filepath = "\"" + filepath + "\"";
       //  classpath = "\"" + classpath + "\"";
       //  javahome = "\"" + javahome + "\"";
       //}

       //String executionString = "java -cp C:\\JDS\\Code\\Henk\\VDBench\\classes vdbench -f " + filepath + " -p " + portNumber;
       //String executionString = javahome + " -cp "+ classpath + " vdbench -f" + filepath + " -p " + portNumber;

       if (OperatingSystemIdentifier.determineOperatingSystem() == OperatingSystemIdentifier.WINDOWS)
         executionString = "\"" + classpath + File.separator +
         "vdbench.bat\" -f \"" + filepath + "\" -g " + portNumber;
       else
         executionString =        classpath + File.separator +
         "vdbench   -f       " + filepath + "   -g " + portNumber;

       /* On Solaris, display Kstat: */
       //if (OperatingSystemIdentifier.determineOperatingSystem() == OperatingSystemIdentifier.SOLARIS)
       //  executionString += " -k";

        // Run VDBench with the assembled parameter file.
        //process = Runtime.getRuntime().exec("java -cp C:\\JDS\\Code\\Henk\\VDBench\\classes vdbench -f " + filepath + " -p " + portNumber);
       process = Runtime.getRuntime().exec(executionString);

        // Make the output pane visible so that user can see output automatically.
        monitorPanel.setOutputPaneVisible();

        // Get the streams associated with the process.
        InputStream errorStream = process.getErrorStream();
        InputStream inputStream = process.getInputStream();

        // Get a reference to the text area used for display.
        JTextArea textArea = monitorPanel.getTextArea();

        // Clear the text area to prepare it to display new data.
        textArea.setText("");

        // Create and start the stream gobblers for asynchronous output.
        StreamGobbler errorGobbler = new StreamGobbler(errorStream, textArea);
        StreamGobbler inputGobbler = new StreamGobbler(inputStream, textArea);
        errorGobbler.start();
        inputGobbler.start();
      }
      catch(SecurityException se)
      {
        System.out.println("executionString: " + executionString);
        JOptionPane.showMessageDialog(null, "Security manager does not allow creation of subprocess.", "Error Starting VDBench", JOptionPane.ERROR_MESSAGE);
        se.printStackTrace();
      }
      catch(IOException ioe)
      {
        System.out.println("executionString: " + executionString);
        ioe.printStackTrace();
        //JOptionPane.showMessageDialog(null, "I/O error occured.", "Error Starting VDBench", JOptionPane.ERROR_MESSAGE);
      }
    }
  }

  // An inner class to listen for the stopRunButton
  private class StopRunActionListener implements ActionListener
  {
    public void actionPerformed(ActionEvent e)
    {
      // Kill the running process and reenable the previously disabled buttons.
      if(process != null)
      {
        multiGraphPanel.closeSocket();
        process.destroy();
        enableButtons(true);
        VDBenchGUI.getGUIFrame().enableFileMenu();
        runParameterPanel.setEditable(true);
      }
    }
  }


  /**
   * Obtain classpath.
   * If there is a concatenation then we always use only the first one.
   * If this is a jar file, then we use the parent of this jar.
   */
  public static String getClassPathDir()
  {
    String classpath_directory_name = null;
    String classpath = System.getProperty("java.class.path");
    boolean windows = (System.getProperty("os.name").toLowerCase().startsWith("windows"));

    /* Remove separator: */
    String tmp = (windows) ? ";" : ":";
    if (classpath.indexOf(tmp) != -1)
      classpath = classpath.substring(0, classpath.indexOf(tmp) );

    try
    {
      classpath_directory_name = new File(classpath).getCanonicalPath();
    }
    catch (Exception e)
    {
      JOptionPane.showMessageDialog(null, e.getMessage(), "Unable to determine classpath directory name: " + System.getProperty("java.class.path"), JOptionPane.ERROR_MESSAGE);
      return null;
    }

    /** If this is a jar file, then we use the parent of this jar.: */
    if (classpath_directory_name.endsWith(".jar"))
      classpath_directory_name = new File(classpath_directory_name).getParent();

    return classpath_directory_name;
  }
  // Inner class to facilitate asynchronous collection and display of
  // output data from VDBench.  Borrowed from SWAT file Command.java.
  private class StreamGobbler extends Thread
  {
    InputStream inputStream;
    JTextArea textArea;

    StreamGobbler(InputStream istream, JTextArea tArea)
    {
      inputStream = istream;
      textArea    = tArea;
    }

    // Intercept output and send it to a text area.
    public void run()
    {
      synchronized(this)
      {
        try
        {
          InputStreamReader streamReader  = new InputStreamReader(inputStream);
          BufferedReader bufferedReader   = new BufferedReader(streamReader);
          String line;

          // Disable buttons which should be non-functional during a run.
          enableButtons(false);
          if(bufferedReader == null)
          {
            return;
          }

          while((line = bufferedReader.readLine()) != null)
          {
            String newdata = "";
            if(textArea != null)
            {
              // Append to the end of the text and scroll to bottom.
              newdata += line + "\n";

              // If there is more data, get it. This avoids the overhead of
              // redisplaying data that will change right away again;
              // however, after 1000 lines display anyway:
              for (int i = 0; i < 1000 && bufferedReader.ready(); i++)
              {
                if ((line = bufferedReader.readLine()) != null)
                {
                  newdata += line + "\n";
                }
              }

              // Limit the amount of data to 100k.
              int len = textArea.getText().length();
              if(len > 100000)
              {
                String txt = "Display buffer too large; truncated from 100k to 90k.\n";
                textArea.setText(txt + textArea.getText().substring(len - 90000));
              }

              textArea.append(newdata);
              textArea.setCaretPosition(textArea.getText().length());
            }
          }
        }
        catch (IOException e)
        {
          //JOptionPane.showMessageDialog(null, "Error Writing Output", e.getMessage(), JOptionPane.ERROR_MESSAGE);
          textArea.append("Error receiving data from Vdbench.");
          //e.printStackTrace();
        }

        notifyAll();

        // Reenable buttons for another run.
        enableButtons(true);
        stopRunButton.setEnabled(false);
        VDBenchGUI.getGUIFrame().enableFileMenu();
        multiGraphPanel.closeSocket();
        runParameterPanel.setEditable(true);

        if(textArea != null)
        {
          textArea.setCaretPosition(textArea.getText().length());
        }
      }
    }
  }
}
