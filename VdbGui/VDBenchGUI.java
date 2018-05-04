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
 * <p>Title: VDBenchGUI</p>
 * <p>Description: This class is a container for elements of the
 * VDBench GUI.  It also runs the application, and includes inner classes
 * for explicit menu functionality.</p>
 * @author Jeff Shafer
 * @version 1.0
 */

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;
import java.io.*;
import java.util.*;
import Utils.Fget;
import Utils.common;
import Utils.Message;


public class VDBenchGUI extends JFrame
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // A reference to an instance of this class, for use in the File and Help Actions.
  private static VDBenchGUI guiFrame;

  // A menubar to hold our menu items.
  private JMenuBar menuBar   = new JMenuBar();

  // File and Help menu objects.
  private JMenu fileMenu     = new JMenu("File");
  private JMenu helpMenu     = new JMenu("Help");

  // Objects corresponding to individual menu items.
  private JMenuItem openItem;
  private JMenuItem saveItem;
  private JMenuItem saveAsItem;
  private JMenuItem exitItem;
  private JMenuItem aboutItem;
  private JMenuItem contactItem;

  // Panels making up the major functional areas of the GUI.
  private static JPanel runOptionsPanel;
  private static ButtonPanel buttonPanel;
  private static WorkloadParameterPanel workloadParameterPanel;
  private static RunParameterPanel runParameterPanel;
  private static MonitorPanel monitorPanel;
  private static MultiGraphPanel multiGraphPanel;
  private static DeviceDisplayPanel devicePanel;

  // Inner action classes to implement menu functionality.
  private FileAction openAction;
  private FileAction saveAction;
  private FileAction saveAsAction;
  private FileAction exitAction;
  private HelpAction aboutAction;
  private HelpAction contactAction;

  // The name of the file which the user has most recently saved or opened.
  private File currentFile;

  // Vector to hold workload defintions.
  private static Vector workloadDefinitions;

  // Vector to hold run definitions.
  private static Vector runDefinitions;

  /**
   * No-argument constructor.
   */
  public VDBenchGUI()
  {
    // Set the application title and build the panel content and menu.
    super("Vdbench 5.01 - GUI");
    buildContentPanels();
    buildMenu();

    // Set the layout and add the panels comprising the application.
    getContentPane().setLayout(new BorderLayout());

    // Add the major panels to the application frame.
    Container container = getContentPane();
    container.add(runOptionsPanel, BorderLayout.NORTH);
    container.add(monitorPanel, BorderLayout.CENTER);

    // Get the tool kit it get the screen size so we can properly
    // position the application on the screen.
    Toolkit kit    = getToolkit();
    Dimension size = kit.getScreenSize();

    // Set an appropriate size and ensure that the frame size is fixed to
    // prevent weird layout behavior upon resizing.
    setSize(1000, 500);
    setResizable(true);

    // Center the application on the user's screen.
    //setLocation(size.width/9, size.height/5);
    centerscreen(this);

    // Create an instance of the device display panel so that it will be ready
    // when it is needed.
    devicePanel = new DeviceDisplayPanel(this);
    buttonPanel.setDeviceDisplayPanel(devicePanel);

    // Create vectors to hold workload and run definitions.
    workloadDefinitions = new Vector();
    runDefinitions      = new Vector();
  }

  /**
   * Centers the screen
   */
  public static void centerscreen(Container container)
  {
    Dimension dim = container.getToolkit().getScreenSize();
    Rectangle abounds = container.getBounds();
    container.setLocation((dim.width - abounds.width) / 2,
                          (dim.height - abounds.height) / 2);
    //container.setVisible(true);
    container.requestFocus();
  }

  /**
   * Provides the caller with a reference to the VDBenchGUI object.
   * @return a reference to the VDBenchGUI object.
   */
  public static VDBenchGUI getGUIFrame()
  {
    return guiFrame;
  }

  /**
   * Allow the file menu to be disabled while VDBench is running.
   */
  public void disableFileMenu()
  {
    fileMenu.setEnabled(false);
  }

  /**
   * Allow the file menu to be enabled while VDBench is not running.
   */
  public void enableFileMenu()
  {
    fileMenu.setEnabled(true);
  }

  /**
   * Allows the other panels to obtain a reference to the monitor panel.
   * @return a reference to the monitor panel.
   */
  public static MonitorPanel getMonitorPanel()
  {
    return monitorPanel;
  }

  /**
   * Allows the other panels to obtain a reference to the run parameter panel.
   * @return a reference to the run parameter panel.
   */
  public static RunParameterPanel getRunParameterPanel()
  {
    return runParameterPanel;
  }

  /**
   * Allows the other panels to obtain a reference to the workload parameter panel.
   * @return a reference to the workload parameter panel.
   */
  public static WorkloadParameterPanel getWorkloadParameterPanel()
  {
    return workloadParameterPanel;
  }

  /**
   * Provides access to workload definition strings.
   * @return a Vector containing the current workload defintions.
   */
  public static Vector getWorkloadDefinitions()
  {
    return workloadDefinitions;
  }

  /**
   * Provides access to run definition strings.
   * @return a Vector containing the current run defintions.
   */
  public static Vector getRunDefinitions()
  {
    return runDefinitions;
  }

  /**
   * Writes a parameter file using the File object f.
   * @param f a File object used to construct the desired parameter file.
   * @throws java.lang.Exception if an invalid run parameter is specified.
   * @throws IOException if an error occurs writing the parameter file.
   */
  public void writeFile(File f) throws Exception, IOException
  {
    PrintWriter dataWriter = null;
    EntryTable deviceList   = guiFrame.getMonitorPanel().getStorageList();
    EntryTable workloadList = guiFrame.getMonitorPanel().getWorkloadList();

    // Check to make sure that we have devices and workloads selected
    // for this file.
    if (deviceList.getRowCount() == 0)
    {
      JOptionPane.showMessageDialog(null, "No devices or files selected.", "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
      return;
    }
    else if (workloadList.getRowCount() == 0)
    {
      JOptionPane.showMessageDialog(null, "No workloads selected.", "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
      return;
    }

    try
    {
      // If the file does not end with the suffix ".txt", make sure that it does!
      if (!f.getName().endsWith(".txt"))
      {
        f = new File(f.getAbsolutePath() + ".txt");
      }

      // Create parmfile.
      dataWriter = new PrintWriter(new FileWriter(f));
      String notificationAlert  = "*******************************************************************************";
      String notificationMsg1   = "*This file has been produced by the Vdbench GUI. Subsequent modification of this*";
      String notificationMsg2   = "*file by hand may render it incompatible for use with the GUI.                *";

      dataWriter.println(notificationAlert);
      dataWriter.println(notificationMsg1);
      dataWriter.println(notificationMsg2);
      dataWriter.println(notificationAlert);

      // Need to add files or storage devices here.
      for (int i = 0; i < deviceList.getRowCount(); i++)
      {
        String storageEntry = (String)deviceList.getValueAt(i, 0);
        dataWriter.println(storageEntry);
      }

      // Add comment about workload defaults.
      String commentString1 = "*Note that all workload parameters have been explicitly specified by the GUI.";
      String commentString2 = "*If parameters not explicity specified, the following defaults apply: rdpct=100,";
      String commentString3 = "*rhpct=0, whpct=0, xfersize=4k, seekpct=100.  Also, file size for newly created files ";
      String commentString4 = "*has been automatically set to a default value of 10 MB.";
      dataWriter.println();
      dataWriter.println(commentString1);
      dataWriter.println(commentString2);
      dataWriter.println(commentString3);
      dataWriter.println(commentString4);
      dataWriter.println();

      // Add all workload entries.
      for (int i = 0; i < workloadDefinitions.size(); i++)
      {
        String workloadEntry = (String)workloadDefinitions.get(i);
        dataWriter.println(workloadEntry);
      }

      // Add line to separate from RD's.
      dataWriter.println();

      // Add run parameters.
      //String runEntry = runParameterPanel.areRunParmsValid();
      for (int i = 0; i < runDefinitions.size(); i++)
      {
        //String workloadEntry = (String)workloadDefinitions.get(i);
        //String workloadEntryName = workloadEntry.substring(workloadEntry.indexOf("=") + 1, workloadEntry.indexOf(",sd=sd*"));
        //dataWriter.println("rd=rd_" + workloadEntryName + ",wd=" + workloadEntryName + ",interval=1" + runEntry);
        dataWriter.println(runDefinitions.get(i));
      }
    }
    catch (IOException ioe)
    {
      throw ioe;
      //JOptionPane.showMessageDialog(null, ioe.getMessage(), "Error Writing Parameter File", JOptionPane.ERROR_MESSAGE);
    }
    /*catch(Exception ex)
    {
      throw ex;
      //JOptionPane.showMessageDialog(null, ex.getMessage(), "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
    }*/
    finally
    {
      dataWriter.close();
    }
  }

  // Build the panel content for the application.
  private void buildContentPanels()
  {
    // Set up the top panel.
    runOptionsPanel = new JPanel(new FlowLayout());
    runOptionsPanel.setBorder(new TitledBorder("Run Options"));

    // Create the subpanels to populate the top panel.
    buttonPanel            = new ButtonPanel();
    workloadParameterPanel = new WorkloadParameterPanel();
    runParameterPanel      = new RunParameterPanel();

    // Create a multigraph panel to display charts.  It is created here,
    // at this time, to create a server socket to facilitate subsequent connection
    // with a socket in VDBench.
    multiGraphPanel = new MultiGraphPanel();
    multiGraphPanel.setDisplayButton(buttonPanel.getDisplayButton());

    // Create the monitor panel.
    monitorPanel = new MonitorPanel();

    // Give button panel references to tables so that it
    // can retrieve data from them as needed.
    buttonPanel.setDeviceList(monitorPanel.getStorageList());
    buttonPanel.setWorkloadList(monitorPanel.getWorkloadList());
    buttonPanel.setRunParameterPanel(runParameterPanel);
    buttonPanel.setWorkloadPanel(workloadParameterPanel);
    buttonPanel.setMonitorPanel(monitorPanel);
    buttonPanel.setMultiGraphPanel(multiGraphPanel);

    // Create separators to divide the subpanels.
    JSeparator s = new JSeparator(JSeparator.VERTICAL);
    JSeparator t = new JSeparator(JSeparator.VERTICAL);

    // Get the dimensions of one of the separators (they're identical).
    Dimension ps = s.getPreferredSize();

    // Add the subpanels and the separators.
    runOptionsPanel.add(buttonPanel);
    runOptionsPanel.add(s);
    runOptionsPanel.add(workloadParameterPanel);
    runOptionsPanel.add(t);
    runOptionsPanel.add(runParameterPanel);

    // Now, set the separator dimensions.  Because the panel into which
    // the separators are being inserted has flow layout, separators are
    // sized according to their UI delegate's preferred size, which is (2, 0).
    // Therefore, one must explicitly set the desired dimensions of the
    // separators; for this case, that is (2, 150).
    s.setPreferredSize(new Dimension(ps.width, 150));
    t.setPreferredSize(new Dimension(ps.width, 150));
  }

  // Builds the help menu for the application.
  private void buildMenu()
  {
    // Set the menu bar and add the help menu itemd.
    setJMenuBar(menuBar);
    menuBar.add(fileMenu);
    menuBar.add(helpMenu);

    // Set the file menu mnemonic.
    fileMenu.setMnemonic('F');

    // Add the "Open" menu item, and set a shortcut.
    openItem = fileMenu.add(openAction = new FileAction("Open", "Open and run a parameter file."));
    //openItem.setAccelerator(KeyStroke.getKeyStroke('O', Event.CTRL_MASK));

    // Divide the menu items, for clarity.
    fileMenu.addSeparator();

    // Add the "Save" menu item, and set a shortcut.
    saveItem = fileMenu.add(saveAction = new FileAction("Save", "Save parameters in file."));
    //saveItem.setAccelerator(KeyStroke.getKeyStroke('S', Event.CTRL_MASK));

    // Add the "Save As..." menu item, and set a shortcut.
    saveAsItem = fileMenu.add(saveAsAction = new FileAction("Save As...", "Save parameters in a named file."));
    //saveAsItem.setAccelerator(KeyStroke.getKeyStroke('A', Event.CTRL_MASK));

    // Divide the menu items, for clarity.
    fileMenu.addSeparator();

    // Add the "Exit" menu item, and set a shortcut.
    exitItem = fileMenu.add(exitAction = new FileAction("Exit", "Exit this application."));
    //exitItem.setAccelerator(KeyStroke.getKeyStroke('X', Event.CTRL_MASK));

    // Set the help menu mnemonic.
    helpMenu.setMnemonic('H');

    // Add the "About" menu item, and set a shortcut.
    aboutItem = helpMenu.add(aboutAction = new HelpAction("About...", "Information about this application."));
    //aboutItem.setAccelerator(KeyStroke.getKeyStroke('A', Event.CTRL_MASK));

    // Divide the menu items, for clarity.
    helpMenu.addSeparator();

    // Add the "Contact" menu item, and set a shortcut.
    //contactItem = helpMenu.add(contactAction = new HelpAction("Contact...", "Send email to VDBench author."));
    //contactItem.setAccelerator(KeyStroke.getKeyStroke('C', Event.CTRL_MASK));
  }

  public static void resetMultiGraphPanel(MultiGraphPanel mgp)
  {
    multiGraphPanel.dispose();
    multiGraphPanel = null;
    multiGraphPanel = mgp;
    multiGraphPanel.setDisplayButton(buttonPanel.getDisplayButton());
    buttonPanel.setMultiGraphPanel(multiGraphPanel);
  }

  // This is the main to run the entire application.
  public static void main(String[] args)
  {
    for (int i = 0; i < args.length; i++)
    {
      if (args[i].startsWith("-d") )
        common.set_debug(Integer.valueOf(args[i].substring(2)).intValue());
    }

    if (!JVMCheck.isJREValid(System.getProperty("java.version"), 1, 5, 0))
    {
      JOptionPane.showMessageDialog(null, "Minimum required Java version for the Vdbench GUI is 1.5.0; \n" +
                                    "You are currently running " + System.getProperty("java.version") +
                                    "\nVdbench terminated",
                                    "Vdbench GUI error", JOptionPane.ERROR_MESSAGE);

      System.exit(-99);
    }

    if (!(System.getProperty("user.name").equals("hv104788") ||
          System.getProperty("user.name").equalsIgnoreCase("henk")))
      Message.infoMsg("The Vdbench GUI supports only a small subset of the complete \n" +
                      "Vdbench functionality, and is mainly to be used as an introduction \n" +
                      "to Vdbench parameter files and how all the parameters work\n" +
                      "together.\n" +
                      "Once you get a little experience it is HIGHLY recommended that \n" +
                      "you start creating your parameter files using your favorite editor and \n" +
                      "then  run those parameter files using './vdbench -f parmfile.name'");

    guiFrame = new VDBenchGUI();

    // Show the frame, and make sure that it behaves properly when closed.
    guiFrame.setVisible(true);

    // Add a window adapter to allow us to shut down properly.
    guiFrame.addWindowListener(
                              new WindowAdapter()
                              {
                                public void windowClosing(WindowEvent e)
                                {
                                  // Kill the process running VDBench and exit.
                                  Process process = buttonPanel.getProcess();
                                  if (process != null)
                                  {
                                    // If we have a process running, just close the socket to kill it.
                                    multiGraphPanel.closeSocket();
                                    //process.destroy();
                                  }
                                  System.exit(0);
                                }
                              }
                              );
  }

  // An inner class responsible for file menu functionality.
  class FileAction extends AbstractAction
  {
    // Constructor; adds the descriptive string to the tooltip.
    FileAction(String name, String toolTip)
    {
      super(name);
      if (toolTip != null)
      {
        putValue(FileAction.SHORT_DESCRIPTION, toolTip);
      }
    }

    public void actionPerformed(ActionEvent e)
    {
      // String to hold path to temporary files, which is where VDBench files
      // created by the GUI are written by default.
      String filePath = "";

      try
      {
        // Create a temporary temp file so that we can determine the directory
        // in which such files are created.  This information is used to set
        // the starting directory for the JFileChooser used to open and save files.
        File tempFile = File.createTempFile("test", "test");

        // Get the path to this file.
        filePath = tempFile.getParent();

        // Since we don't need the file anymore, we delete it.
        tempFile.delete();
      }
      catch (IOException ioe)
      {
        // If there was a problem creating the temp file, better exit.
        ioe.printStackTrace();
        System.exit(-1);
      }

      // Create the file chooser with the appropriate path.
      JFileChooser chooser = new JFileChooser(filePath);
      chooser.setFileFilter(new TextFilter());
      chooser.setMultiSelectionEnabled(false);

      // If the user elects to open a file...
      if (e.getSource() == openItem)
      {
        // If a file was previously saved in this session, open the chooser in
        // the same directory as before.
        if (currentFile != null && currentFile.getParentFile() != null)
        {
          chooser.setCurrentDirectory(new File(currentFile.getParentFile().getAbsolutePath()));
        }

        // ...determine which button the user presses: "Open", or "Cancel".
        int openChoice = chooser.showOpenDialog(guiFrame);

        // If the user elects to cancel, simply do nothing.
        if (openChoice == JFileChooser.CANCEL_OPTION)
        {
          return;
        }

        // Otherwise, get the File object corresponding to the selected file.
        File file = chooser.getSelectedFile();

        // If the user enters an invalid filename, keep prompting him for a valid one until he complies or cancels.
        while (!file.exists() && openChoice == JFileChooser.APPROVE_OPTION)
        {
          JOptionPane.showMessageDialog(guiFrame, "Invalid File Name", "Please enter a valid filename.", JOptionPane.ERROR_MESSAGE);
          openChoice = chooser.showOpenDialog(guiFrame);
          file = chooser.getSelectedFile();
        }

        // If we have a legitimate file and the user chooses "Open"...
        if (file.exists() && openChoice == JFileChooser.APPROVE_OPTION)
        {
          // See if the file has a header, so we know if it was created by this application.
          validateFileHeader(file);

          // Remove previous entries from panels.
          guiFrame.getMonitorPanel().clearPanels();

          // Clear workload and run vectors.
          workloadDefinitions.clear();
          runDefinitions.clear();

          // Remember this file as the current file.
          currentFile = file;

          // Read from the file and display the information on the GUI.
          Fget fg = new Fget(currentFile);

          String line;

          while ((line = fg.get()) != null)
          {
            processOpenFile(line);
          }

          // Write the file's info to the panel
          workloadParameterPanel.writeFileToPanel();
        }
      }
      try
      {
        // Validate run parameters.
        //runParameterPanel.areRunParmsValid();

        // If the user selects the "Save" menu item...
        if (e.getSource() == saveItem)
        {
          // Make sure that device/file and workload parameters have been defined.
          if (!areParametersPresent(monitorPanel))
          {
            return;
          }

          // If the user has opened a file before, or saved a file...
          if (currentFile != null)
          {
            //...ask if he wishes to overwrite that file.
            int confirmChoice = JOptionPane.showConfirmDialog(guiFrame,
                                                              "Do you wish to overwrite " + currentFile.getName() + "?",
                                                              "File Overwrite Warning", JOptionPane.YES_NO_OPTION);

            // If he does not wish to overwrite...
            if (confirmChoice == JOptionPane.NO_OPTION)
            {
              // ...give him the option to save with a new name, in the same
              // directory as before.
              if (currentFile.getParentFile() != null)
              {
                chooser.setCurrentDirectory(new File(currentFile.getParentFile().getAbsolutePath()));
              }
              int renameChoice = chooser.showSaveDialog(guiFrame);

              // If the user decides to cancel, do nothing.
              if (renameChoice == JFileChooser.CANCEL_OPTION)
              {
                return;
              }
              // If he types in a new file name, set it as the current file and write the file.
              if (renameChoice == JFileChooser.APPROVE_OPTION)
              {
                File file = chooser.getSelectedFile();

                currentFile = file;
                writeFile(currentFile);
              }
            }
            // Otherwise, if he wishes to overwrite in the first place,
            // just write the file.
            if (confirmChoice == JOptionPane.YES_OPTION)
            {
              writeFile(currentFile);
            }
          }
          // If the user has not opened or saved a file thus far during this session...
          else
          {
            //...give him the opportunity to choose a name and save.
            int saveChoice = chooser.showSaveDialog(guiFrame);

            // If the user elects to cancel, simply do nothing.
            if (saveChoice == JFileChooser.CANCEL_OPTION)
            {
              return;
            }

            // If the user elects to save the file, store this file's
            // name as the current file and write the file.
            if (saveChoice == JFileChooser.APPROVE_OPTION)
            {
              File file = chooser.getSelectedFile();

              currentFile = file;
              writeFile(currentFile);
            }
          }
        }

        // If the user selects the "Save As..." menu item...
        if (e.getSource() == saveAsItem)
        {
          // Make sure that device/file and workload parameters have been defined.
          if (!areParametersPresent(monitorPanel))
          {
            return;
          }

          // If a file was previously saved in this session, open the chooser in
          // the same directory as before.
          if (currentFile != null && currentFile.getParentFile() != null)
          {
            chooser.setCurrentDirectory(new File(currentFile.getParentFile().getAbsolutePath()));
          }

          int saveAsChoice = chooser.showSaveDialog(guiFrame);

          // If the user decides to save the current parameters,
          // set the current file accordingly.
          if (saveAsChoice == JFileChooser.APPROVE_OPTION)
          {
            File file = chooser.getSelectedFile();

            currentFile = file;
            writeFile(currentFile);
          }
          // If the user decides to cancel, do nothing.
          if (saveAsChoice == JFileChooser.CANCEL_OPTION)
          {
            return;
          }
        }

        // If the user selects the "Exit" menu item...
        if (e.getSource() == exitItem)
        {
          // Pull the plug on the sockets and kill the process.
          multiGraphPanel.closeSocket();
          if (buttonPanel.getProcess() != null)
          {
            buttonPanel.getProcess().destroy();
          }
          System.exit(0);
        }
      }
      catch (IOException ioe)
      {
        JOptionPane.showMessageDialog(null, ioe.getMessage(), "Error Writing Parameter File", JOptionPane.ERROR_MESSAGE);
        ioe.printStackTrace();
        return;
      }
      catch (Exception ex)
      {
        JOptionPane.showMessageDialog(null, ex.getMessage(), "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
        ex.printStackTrace();
        return;
      }
    }

    // Let the user know if the file header is not present.
    private void validateFileHeader(File file)
    {
      // Check for file header.
      try
      {
        Fget fg = new Fget(file);

        String line;

        if ((line = fg.get()) != null)
        {
          if (!line.equals("*******************************************************************************"))
          {
            // This exception contains information specific to the matter of file modification, and it will be caught
            // by the second catch statement below.
            throw new Exception("Potentially invalid file.  This file may not be compatible with this application.");
          }
        }
        fg.close();
      }
      catch (IOException ioe)
      {
        // If the user selects a file that is of the wrong type, let him know.
        JOptionPane.showMessageDialog(guiFrame, "There has been an error opening the requested file.", "Error Opening File", JOptionPane.ERROR_MESSAGE);
        ioe.printStackTrace();
      }
      catch (Exception ex)
      {
        JOptionPane.showMessageDialog(guiFrame, ex.getMessage(), "Error Opening File", JOptionPane.ERROR_MESSAGE);
        ex.printStackTrace();
      }
    }

    // Confirm that we have entries to save before we save to file.
    private boolean areParametersPresent(MonitorPanel mPanel)
    {
      boolean status = true;

      // Check to make sure that we have devices and workloads selected
      // for this run.
      if (mPanel.getStorageList().getRowCount() == 0)
      {
        JOptionPane.showMessageDialog(null, "No devices or files selected.", "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
        status = false;
      }
      if (mPanel.getWorkloadList().getRowCount() == 0)
      {
        JOptionPane.showMessageDialog(null, "No workloads selected.", "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
        status = false;
      }
      return status;
    }

    // Places data from the opened file in the appropriate fields of the gui.
    private void processOpenFile(String s)
    {
      // If we have an SD...
      if (s.startsWith("sd="))
      {
        guiFrame.getMonitorPanel().addDeviceEntry(s);
      }
      // If we have a WD...
      if (s.startsWith("wd="))
      {
        //guiFrame.getMonitorPanel().addWorkloadEntry(s);
        workloadDefinitions.add(s);
      }
      // If we have an RD...
      if (s.startsWith("rd="))
      {
        runDefinitions.add(s);
        //setRunParameters(s);
      }
    }

    // Parse out the elements of a run definition string.
    private void setRunParameters(String s)
    {
      String duration = "";
      String ioRate   = "";
      String threads  = "";

      // Split off the portion of the string which contains thread information.
      // This gives the index of the last equal sign in the run definition,
      // which is right after the keyword "forthreads".
      int threadStringIndex = s.lastIndexOf("=");

      // We must cut off the character string ",forthreads" from the main string,
      // so we are only left with the info we want, separated by commas.
      // Thus, we subtract 11 from the last index of "=" so that this string does
      // not include the substring ",forthreads".
      String mainParameters = s.substring(0, threadStringIndex - 11);

      // The thread string, which can contain multiple thread numbers, separated
      // by commas, begins after the last "=", so we must add one to the index to get it.
      String threadParameters = s.substring(threadStringIndex + 1);

      // Process the duration and I/O rate information.
      StringTokenizer st = new StringTokenizer(mainParameters, ",");
      while (st.hasMoreElements())
      {
        String element = st.nextToken();

        if (element.startsWith("elapsed="))
        {
          duration = element.substring(8);
        }
        if (element.startsWith("iorate="))
        {
          ioRate = element.substring(7);
        }
      }

      // If the threads string in the file contains parentheses, strip them out.
      if (threadParameters.startsWith("("))
      {
        threads = threadParameters.substring(1, threadParameters.length()-1);
      }

      // Place the strings in the panel.
      guiFrame.getRunParameterPanel().setFields(threads, duration, ioRate);
    }

    // Parse out the elements of a workload definition string.
    private void parseWorkloadParameters(String s)
    {
      String workloadName;
      String rdPct;
      String rhPct;
      String whPct;
      String xferSize;
      String seekPct;

      StringTokenizer st = new StringTokenizer(s, ",");
      while (st.hasMoreElements())
      {
        String element = st.nextToken();

        if (element.startsWith("wd="))
        {
          workloadName = element.substring(3);
          workloadName.replace('_', ' ');
        }
        if (element.startsWith("rdpct="))
        {
          rdPct = element.substring(6);
        }
        if (element.startsWith("rhpct="))
        {
          rhPct = element.substring(6);
        }
        if (element.startsWith("whpct="))
        {
          whPct = element.substring(6);
        }
        if (element.startsWith("xfersize="))
        {
          xferSize = element.substring(9);
        }
        if (element.startsWith("seekpct="))
        {
          seekPct = element.substring(9);
        }
      }
    }
  }

  // An inner class responsible for help menu functionality.
  class HelpAction extends AbstractAction
  {
    // Constructor; adds the descriptive string to the tooltip.
    HelpAction(String name, String toolTip)
    {
      super(name);
      if (toolTip != null)
      {
        putValue(SHORT_DESCRIPTION, toolTip);
      }
    }

    public void actionPerformed(ActionEvent e)
    {
      if (e.getSource() == aboutItem)
      {
        // Create a dialog window with the application as parent.
        String classPath = System.getProperty("java.class.path");
        String javaDirectory = System.getProperty("java.home");
        String [] info = {
          "Java Home: " + javaDirectory,
          "Please run this application using Java version 1.5.0, or higher."};
        AboutDialog aboutDialog = new AboutDialog(guiFrame, "About Vdbench GUI", info);
        aboutDialog.setVisible(true);
      }

      if (e.getSource() == contactItem)
      {
        // Create a dialog window with contact information.
        String [] contactString = {"For further information or assistance, contact Henk Vandenbergh at hv@sun.com."};
        AboutDialog contactDialog = new AboutDialog(guiFrame, "Contact Information", contactString);
        contactDialog.setVisible(true);
      }
    }
  }
}
