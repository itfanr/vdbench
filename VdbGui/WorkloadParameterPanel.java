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
 * <p>Title: WorkloadParameterPanel.java</p>
 * <p>Description: This class contains the GUI input elements.</p>
 * @author Jeff Shafer
 * @version 1.0
 * Date: 5/07/04
 */

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.table.*;
import java.util.*;
import java.io.*;

public class WorkloadParameterPanel extends JPanel implements ActionListener, ItemListener
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static int entryCount = 1;

  // This is the number of anonymous subpanels used to layout the
  // workload parameter textfields and corresponding labels.
  private final static int FIELD_NUMBER = 12;

  // Vector to hold predefined workload objects.
  Vector v = new Vector();

  // Object to hold list of predefined workloads.
  private JComboBox predefinedWorkloadList;

  // Button to add workloads to workload pane.
  private JButton setWorkloadButton = new JButton("Commit Workload");

  // Text fields for basic workload parameters.
  private JTextField workloadNameField = new JTextField(10);
  private JTextField writeHitField     = new JTextField(10);
  private JTextField readField         = new JTextField(10);
  private JTextField randomField       = new JTextField(10);
  private JTextField readHitField      = new JTextField(10);
  private JTextField transferSizeField = new JTextField(10);

  // Values for workload parameters.
  private String workloadName;
  private int writeHitPct;
  private int readPct;
  private int randomPct;
  private int readHitPct;
  private String[] transferSize;

  // Labels for text fields.
  private JLabel workloadNameLabel = new JLabel("Workload Name");
  private JLabel writeHitLabel     = new JLabel("Write Hit Percentage");
  private JLabel readLabel         = new JLabel("Read Percentage");
  private JLabel randomLabel       = new JLabel("Random Percentage");
  private JLabel readHitLabel      = new JLabel("Read Hit Percentage");
  private JLabel transferSizeLabel = new JLabel("Transfer Size");

  // Subpanels used to display labels and text fields.
  private JPanel [] fieldPanel           = new JPanel[FIELD_NUMBER];
  private JPanel [] componentPanel       = new JPanel[2];
  private JPanel componentContainerPanel = new JPanel(new GridLayout(1, 2, 5, 5));
  private JPanel parameterContainerPanel = new JPanel(new GridLayout(3, 4, 5, 5));

  // String to hold workload entries as they are created.
  private String workloadEntryString;

  /**
   * No-argument constructor.
   */
  public WorkloadParameterPanel()
  {
    // Set a titled border and set layout to border layout.
    setLayout(new BorderLayout());

    // Create the predefined workloads.
    createWorkloads();

    // Create and populate the subpanels which comprise this panel.
    buildListAndButtonPanel();
    buildFieldPanels();

    // Set values in this panel corresponding to the default predefined workload.
    displayWorkloadValues((VDBWorkload)v.get(0));

    // Add the subpanels to this panel.
    add(componentContainerPanel, BorderLayout.NORTH);
    add(parameterContainerPanel, BorderLayout.CENTER);

    // Set the tool tip text.
    setToolTips();

    // Set the listeners.
    setWorkloadButton.addActionListener(this);
    predefinedWorkloadList.addItemListener(this);
  }

  /**
   * Allows the caller to obtain a reference to the button used to commit workloads.
   * @return a reference to the button used to commit workloads.
   */
  public JButton getWorkloadButton()
  {
    return setWorkloadButton;
  }

  // Listens to set workload button and adds workloads to the workload pane.
  public void actionPerformed(ActionEvent e)
  {
    // If the workload is valid, add it to the workload monitor panel.
    if(isWorkloadValid())
    {
      writeWorkloadToPanel();
    }
  }

  // Listens to predefined workload drop-down list.
  public void itemStateChanged(ItemEvent e)
  {
    VDBWorkload currentWorkload = (VDBWorkload)predefinedWorkloadList.getSelectedItem();
    displayWorkloadValues(currentWorkload);
  }

  /**
   * Sets values in parameter fields to those contained in the String argument, s.
   * This allows the user to left click on a line and have the values in that line
   * appear for editing in the parameter panel.
   * @param s a string containing a workload definition.
   * @param row an integer specifing the row number of the wd in the list of workload defs.
   */
  public void setFields(String s, int row)
  {
    // Need a flag to tell us if the string "xfersize" has
    // been seen in this string.  That way, if it is not present,
    // we will know that we have multiple transfer sizes and must
    // get the string from the run parameter set.
    boolean isXferSizePresent = false;

    StringTokenizer st = new StringTokenizer(s, ",");
    while(st.hasMoreTokens())
    {
      String entry = st.nextToken();

      if(entry.startsWith("wd"))
      {
        String name = entry.substring(3);
        name = name.replace('_', ' ');
        workloadNameField.setText(name);
      }
      if(entry.startsWith("rdpct"))
      {
        String rdpct = entry.substring(6);
        readField.setText(rdpct);
      }
      if(entry.startsWith("rhpct"))
      {
        String rhpct = entry.substring(6);
        readHitField.setText(rhpct);
      }

      if(entry.startsWith("whpct"))
      {
        String whpct = entry.substring(6);
        writeHitField.setText(whpct);
      }

      if(entry.startsWith("xfersize"))
      {
        isXferSizePresent = true;
        String xfer = entry.substring(9, entry.length()-1);
        transferSizeField.setText(xfer);
      }

      if(entry.startsWith("seekpct"))
      {
        String seek = entry.substring(8);
        randomField.setText(seek);
      }
    }

    // If we have not seen the transfer size keyword, get xfer sizes
    // from the run parameter set.
    if(!isXferSizePresent)
    {
      Vector runs = VDBenchGUI.getRunDefinitions();
      String runString = (String)runs.get(row);
      int xferIndex = runString.indexOf("forxfersize=(");
      String xferString = "";
      if(xferIndex != -1)
      {
        runString = runString.substring(runString.indexOf("forxfersize=("));
        xferString = runString.substring(runString.indexOf("(") + 1, runString.indexOf(")"));

        StringTokenizer tokenizer = new StringTokenizer(xferString, "k");
        String xferSizes = "";

        xferSizes = xferString;

        //while(tokenizer.hasMoreTokens())
        //{
        //  xferSizes += tokenizer.nextToken();
        //}

        System.out.println("xx: " + xferString);
        transferSizeField.setText(xferString);
      }
    }
  }

  /**
   * In the event that fields contain values of a workload that was
   * deleted, reset all fields with the default workload values.
   */
  public void refreshFields()
  {
    displayWorkloadValues((VDBWorkload)v.get(0));
  }

  // Write the workload from file to the panel.
  public void writeFileToPanel()
  {
    // Get the monitor panel and go through the list of workload names.
    // If we find a name that matches, replace it with the new workload.
    Vector workloads = VDBenchGUI.getWorkloadDefinitions();
    Vector runs      = VDBenchGUI.getRunDefinitions();

    // Make sure the workload pane is visible.
    (VDBenchGUI.getMonitorPanel()).setWorkloadPaneVisible();

    // Write the workloads and corresponding runs to the monitor panel.
    VDBenchGUI.getMonitorPanel().writeEntriesToPanel(workloads, runs);

    // Parse a run entry to extract values for run parameters.  This is
    // OK to do since run parameters are the same for all workloads.
    String runString = (String)runs.get(0);

    String valueString = runString.substring(runString.indexOf("iorate="));
    //System.out.println("1ValueString: " + valueString);

    String ioString = valueString.substring(valueString.indexOf("iorate=") + "iorate=".length(), valueString.indexOf(","));
    //System.out.println("1IOString: " + ioString);

    valueString = valueString.substring(valueString.indexOf("elapsed="));
    String durationString = valueString.substring(valueString.indexOf("elapsed=") + "elapsed=".length(), valueString.indexOf(","));
    //System.out.println("1DurationString: " + durationString);

    valueString = valueString.substring(valueString.indexOf("forthreads="));
    String threadString = valueString.substring(valueString.indexOf("forthreads=(") + "forthreads=(".length(), valueString.indexOf(")"));
    //System.out.println("1ThreadString: " + threadString);

    // Write the run values to the run fields.
    VDBenchGUI.getRunParameterPanel().setFields(threadString, durationString, ioString);
  }


  // Write the validated workload to the workload panel.
  private void writeWorkloadToPanel()
  {
    // Get the monitor panel and go through the list of workload names.
    // If we find a name that matches, replace it with the new workload.
    Vector workloads = VDBenchGUI.getWorkloadDefinitions();
    Vector runs      = VDBenchGUI.getRunDefinitions();

    // First, get the number of entries, and the model containing those entries.
    //int numberOfEntries = VDBenchGUI.getMonitorPanel().getWorkloadList().getRowCount();
    //TableModel workloadListModel = VDBenchGUI.getMonitorPanel().getWorkloadList().getModel();

    String runEntry = null;
    if(VDBenchGUI.getRunParameterPanel().areRunParmsValid())
    {
      runEntry = VDBenchGUI.getRunParameterPanel().getEntries();
    }
    else
    {
      return;
    }

    String runEntryString;

    // Make sure the workload pane is visible.
    (VDBenchGUI.getMonitorPanel()).setWorkloadPaneVisible();

    // Look at each entry.
    //for(int i = 0; i < numberOfEntries; i++)
    for(int i = 0; i < workloads.size(); i++)
    {
      // Get the entry and parse it into comma-separated strings.
      //String entry = (String)workloadListModel.getValueAt(i, 0);
      String entry = (String)workloads.get(i);
      StringTokenizer st = new StringTokenizer(entry, ",");

      // Get the first token, which contains the workload name.
      String name = st.nextToken();
      name = name.substring(3);

      // If the name equals a name already in the list, and the new workload with the
      // same name is valid, replace the old workload with the new workload with the same name.
      if(name.equals(formatWorkloadName(workloadName)))// && isWorkloadValid())
      {
        // Create a workload string for each valid transfer size.
        workloadEntryString = "wd=" + formatWorkloadName(workloadName) + ",sd=sd*,";
        workloadEntryString += "rdpct=" + readPct + ",";
        workloadEntryString += "rhpct=" + readHitPct + ",";
        workloadEntryString += "whpct=" + writeHitPct + ",";

        runEntryString = "rd=rd_" + formatWorkloadName(workloadName) + ",wd=" + formatWorkloadName(workloadName) + ",interval=1" + runEntry;

        // If we only have a single transfer size, it must appear in the workload
        // definition.  Otherwise, it appears in the run definition.
        if(transferSize.length == 1)
        {
          workloadEntryString += "xfersize=" + transferSize[0] + ",";

        }
        else
        {
          runEntryString += ",forxfersize=(";
          //runEntry += ",forxfersize=(";
          for(int j = 0; j < transferSize.length; j++)
          {
            if(j == transferSize.length - 1)
            {
              runEntryString += transferSize[j];
              //runEntry += transferSize[j] + "k";
            }
            else
            {
              runEntryString += transferSize[j] + ",";
              //runEntry += transferSize[j] + "k,";
            }
          }
          runEntryString += ")";
          //runEntry += ")";
        }
        workloadEntryString += "seekpct=" + randomPct;
        //runEntryString += runEntry;

        // Add the workload entry to the list and return since we are done.
        //workloadListModel.setValueAt(workloadEntryString, i, 0);
        //System.out.println("Workload: " + workloadEntryString);
        //System.out.println("Run: " + runEntryString);
        workloads.setElementAt(workloadEntryString, i);
        runs.setElementAt(runEntryString, i);

        for(int j = 0; j < runs.size(); j++)
        {
          // Get the run string.
          String runString = (String)runs.get(j);

          // Get the stuff prior to the input run information.
          String preString = runString.substring(0, runString.indexOf(",iorate"));

          // If this run contains xfersize information, extract and store it.
          String xfersizeString = "";
          if(runString.indexOf(",forxfersize") != -1)
          {
            xfersizeString = runString.substring(runString.indexOf(",forxfersize="));
          }

          // Now, rebuild the string with the new run entry information.
          String newEntry = preString + runEntry + xfersizeString;

          // Add the updated entry to the run vector.
          runs.setElementAt(newEntry, j);
        }
        VDBenchGUI.getMonitorPanel().writeEntriesToPanel(workloads, runs);
        return;
      }
    }

    // In this case, no workload exits with the current name, so add this one
    // at the end.
    // Create a workload string for each valid transfer size.
    workloadEntryString = "wd=" + formatWorkloadName(workloadName) + ",sd=sd*,";
    workloadEntryString += "rdpct="    + readPct         + ",";
    workloadEntryString += "rhpct="    + readHitPct      + ",";
    workloadEntryString += "whpct="    + writeHitPct     + ",";

    runEntryString = "rd=rd_" + formatWorkloadName(workloadName) + ",wd=" + formatWorkloadName(workloadName) + ",interval=1" + runEntry;

    // If we only have a single transfer size, it must appear in the workload
    // definition.  Otherwise, it appears in the run definition.
    if(transferSize.length == 1)
    {
      workloadEntryString += "xfersize=" + transferSize[0] + ",";
    }
    else
    {
      runEntryString += ",forxfersize=(";
      //runEntry += ",forxfersize=(";
      for(int j = 0; j < transferSize.length; j++)
      {
        if(j == transferSize.length - 1)
        {
          runEntryString += transferSize[j];
          //runEntry += transferSize[j] + "k";
        }
        else
        {
          runEntryString += transferSize[j] + ",";
          //runEntry += transferSize[j] + "k,";
        }
      }
      runEntryString += ")";
      //runEntry += ")";
    }

    workloadEntryString += "seekpct="  + randomPct;
    //runEntryString += runEntry;

    // Otherwise, this workload entry is new, so just add it as a new entry.
    //System.out.println("Workload: " + workloadEntryString);
    //System.out.println("Run: " + runEntryString);
    workloads.add(workloadEntryString);
    runs.add(runEntryString);

    for(int j = 0; j < runs.size(); j++)
    {
      // Get the run string.
      String runString = (String)runs.get(j);
      //System.out.println("runString: " + runString);

      // Get the stuff prior to the input run information.
      String preString = runString.substring(0, runString.indexOf(",iorate"));
      //System.out.println("preString: " + preString);

      // If this run contains xfersize information, extract and store it.
      String xfersizeString = "";
      if(runString.indexOf(",forxfersize") != -1)
      {
        xfersizeString = runString.substring(runString.indexOf(",forxfersize="));
      }
      //System.out.println("xfersizeString: " + xfersizeString);

      // Now, rebuild the string with the new run entry information.
      String newEntry = preString + runEntry + xfersizeString;
      //System.out.println("newEntry: " + newEntry);

      // Add the updated entry to the run vector.
      runs.setElementAt(newEntry, j);
    }

    VDBenchGUI.getMonitorPanel().writeEntriesToPanel(workloads, runs);

    // Add each workload string to the workload pane for display.
    //(VDBenchGUI.getMonitorPanel()).addWorkloadEntry(workloadEntryString);

    // Now that we have properly updated the workload vector, lets rebuild the
    // run defintion vector.
  }



  // Format the workload name so that it has no blank spaces.
  // All blanks are replaced with underscores.
  private String formatWorkloadName(String name)
  {
    name = name.trim();
    return name.replace(' ', '_');
  }

  // Extract workload parameters from GUI, validate them, and store them.
  private boolean isWorkloadValid()
  {
    // Assume that the workload is not valid; if this is the case, this will
    // propagate through to the "finally" block below.
    boolean isValid = false;

    // Verify values which require it and notify the user with a detailed
    // message via JOptionPane.
    try
    {
      workloadName = workloadNameField.getText();
      writeHitPct  = verifyPercentage(writeHitField, "write hit percentage");
      readPct      = verifyPercentage(readField, "read percentage");
      randomPct    = verifyPercentage(randomField, "random percentage");
      readHitPct   = verifyPercentage(readHitField, "read hit percentage");
      transferSize = verifyXferSize(transferSizeField, "transfer size");

      // If all the values are valid, return true so we will know it is OK
      // to write the workload parameters to the appropriate monitor panel.
      isValid = true;
    }
    catch(NumberFormatException nfe)
    {
      // If the user entered a non-numeric value, notify him.
      JOptionPane.showMessageDialog(this, nfe.getMessage(), "Invalid Workload Parameter", JOptionPane.ERROR_MESSAGE);
    }
    catch(IllegalArgumentException iae)
    {
      // If the user entered a numeric value which is illegal in the present context, notify him.
      JOptionPane.showMessageDialog(this, iae.getMessage(), "Invalid Workload Parameter", JOptionPane.ERROR_MESSAGE);
    }
    //finally
    {
      // Need to return a value, whether or not an exception was thrown.
      return isValid;
    }
  }

  // Verifies that value entered in the input text field with the associated field name
  // is indeed a valid percentage (>= 0.0  and <= 100.0), and returns that value as a double.
  private int verifyPercentage(JTextField field, String fieldName)
  {
    int value = 0;
    try
    {
      // If the field value cannot be parsed as a double, a NumberFormatException is thrown.
      // However, in that case, we replace the NumberFormatException with a new one below,
      // containing specific information about the field which caused the problem.
      value = Integer.parseInt(field.getText());

      // Check to make sure that the value entered is a number appropriate for a
      // percentage in this context (from 0 and 100, inclusive).  If not, throw
      // an illegal argument exception.
      if (value < 0 || value > 100)
      {
        throw new IllegalArgumentException("Workload " + fieldName + " out of range.  Value must be an integer, >= 0 and <= 100");
      }
    }
    catch(NumberFormatException nfe)
    {
      // This replaces the original NumberFormatException thrown by "parseDouble".
      throw new NumberFormatException("Workload parameter " + fieldName + " is a non-numeric value.");
    }
    return value;
  }

  // Verifies that value(s) entered in the input text field with the associated field name
  // is indeed a valid transfer size (> 0.0), and returns value(s) as double(s).
  private String[] verifyXferSize(JTextField field, String fieldName)
  {
    StringTokenizer st = new StringTokenizer(transferSizeField.getText(), ", \t\n\r\f");
    int numberOfTokens = st.countTokens();

    // Check to make sure we have at least one value.
    if(numberOfTokens == 0)
    {
      throw new IllegalArgumentException("A transfer size must be specified.");
    }

    String[] valueArray = new String[numberOfTokens];
    String displayString = "";

    // Get the entries, and examine each in turn.
    for(int i = 0; i < numberOfTokens; i++)
    {
      // If we have multiple arguments, separate them in the display string using commas.
      if(i > 0)
      {
        displayString += ",";
      }

      valueArray[i] = st.nextToken();
      displayString += valueArray[i];
    }

    // Check for duplicate values.
    for(int i = 0; i < valueArray.length; i++)
    {
      for(int j = i + 1; j < valueArray.length; j++)
      {
        if(valueArray[i] == valueArray[j])
        {
          throw new IllegalArgumentException("A transfer size has been duplicated.");
        }
      }
    }

    transferSizeField.setText(displayString);
    return valueArray;
  }

  // Creates the predefined workloads for the application.
  private void createWorkloads()
  {
    // Create a file object corresponding to the file
    // containing the workload definitions.
    //File workloadFile = new File(".", "workloadDefinitions.txt");
    //File workloadFile = new File("workloadDefinitions.txt");

    // If the file does not exist, notify the user and kill the application.
    //if(!workloadFile.exists())
    //{
    //  JOptionPane.showMessageDialog(null, "Workload Definitions Missing",
    //                                "Workload definition file not found.",
    //                                JOptionPane.ERROR_MESSAGE);
    //  System.exit(1);
    //}

    //try
    //{
      // Read each line from the input file and store it in a vector.
      //BufferedReader input = new BufferedReader(new FileReader(workloadFile));
      //String line;

      //while((line = input.readLine()) != null)
      //{
        //v.add(new VDBWorkload(line));
      //}
    //}
    //catch(IOException ioe)
    //{
      // If we have trouble reading the file, let the user know and terminate the application.
      //JOptionPane.showMessageDialog(null, "Line improperly read from input file.", "Error Reading From File", JOptionPane.ERROR_MESSAGE);
      //System.exit(1);
    //}

    v.add(new VDBWorkload("Read Hit",0,100,100,100,"4k"));
    v.add(new VDBWorkload("Read Miss",0,100,0,100,"4k"));
    v.add(new VDBWorkload("Write Hit",100,0,0,100,"4k"));
    v.add(new VDBWorkload("Write Miss",0,0,0,100,"4k"));
    v.add(new VDBWorkload("Sequential Read",0,100,0,0,"4k"));
    v.add(new VDBWorkload("Sequential Write",0,0,0,0,"4k"));


    // Create the predefined workload list with the vector.
    predefinedWorkloadList = new JComboBox(v);
  }

  // Sets the workload values seen in this panel.
  private void displayWorkloadValues(VDBWorkload workload)
  {
    workloadNameField.setText(workload.toString());
    writeHitField.setText(Integer.toString(workload.getWriteHitPct()));
    readField.setText(Integer.toString(workload.getReadPct()));
    randomField.setText(Integer.toString(workload.getRandomPct()));
    readHitField.setText(Integer.toString(workload.getReadHitPct()));
    transferSizeField.setText(workload.getxferSize());
  }

  // Sets the tool tip text for display.
  private void setToolTips()
  {
    // Set tool tips for drop-down list and for button.
    predefinedWorkloadList.setToolTipText("Selects a predefined workload, which may be modified.");
    setWorkloadButton.setToolTipText("Adds the displayed workload to the \"Workload(s)\" panel.");

    // Set tool tips for parameter fields.
    workloadNameField.setToolTipText("Enter a name for the current workload.");
    writeHitField.setToolTipText("Enter a write hit percentage, without percent sign.");
    readField.setToolTipText("Enter a read percentage, without percent sign.");
    randomField.setToolTipText("Enter a random percentage, without percent sign.");
    readHitField.setToolTipText("Enter a read hit percentage, without percent sign.");
    transferSizeField.setToolTipText("Enter transfer size(s) in bytes, e.g. 512,4k, 2m.");
  }

  // Builds the predefined workload drop-down list and adds
  // it and the set workload button to the component panel.
  private void buildListAndButtonPanel()
  {
    componentPanel[0] = new JPanel();
    componentPanel[1] = new JPanel();
    componentPanel[0].setLayout(new GridLayout(1, 2));
    JPanel listLabelPanel = new JPanel();
    listLabelPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
    listLabelPanel.add(new JLabel("Select Workload"));
    JPanel listPanel = new JPanel();
    listPanel.setLayout(new FlowLayout(FlowLayout.RIGHT));
    listPanel.add(predefinedWorkloadList);
    componentPanel[0].add(listLabelPanel);
    componentPanel[0].add(listPanel);
    //componentPanel[0].add(new JLabel("Select Workload"));
    //componentPanel[0].add(predefinedWorkloadList);
    componentPanel[1].setLayout(new FlowLayout(FlowLayout.RIGHT));
    componentPanel[1].add(setWorkloadButton);
    componentContainerPanel.add(componentPanel[0]);
    componentContainerPanel.add(componentPanel[1]);
  }

  // Builds the field panels containing labels and text fields.
  private void buildFieldPanels()
  {
    // Create subpanels to contain each individual
    // label and text field, and add them to the parameter panel.
    for(int i = 0; i < FIELD_NUMBER; i++)
    {
      fieldPanel[i] = new JPanel();
      if(i % 2 == 0)
      {
        fieldPanel[i].setLayout(new FlowLayout(FlowLayout.RIGHT));
      }
      else
      {
        fieldPanel[i].setLayout(new FlowLayout(FlowLayout.RIGHT));
      }
      parameterContainerPanel.add(fieldPanel[i]);
    }

    // Add the individual labels and fields to their
    // respective subpanels.
    fieldPanel[0].add(workloadNameLabel);
    fieldPanel[1].add(workloadNameField);
    fieldPanel[2].add(writeHitLabel);
    fieldPanel[3].add(writeHitField);
    fieldPanel[4].add(readLabel);
    fieldPanel[5].add(readField);
    fieldPanel[6].add(randomLabel);
    fieldPanel[7].add(randomField);
    fieldPanel[8].add(readHitLabel);
    fieldPanel[9].add(readHitField);
    fieldPanel[10].add(transferSizeLabel);
    fieldPanel[11].add(transferSizeField);
  }

  // A short main to test.
  public static void main(String [] args)
  {
    WorkloadParameterPanel testPanel = new WorkloadParameterPanel();
    JFrame testFrame = new JFrame();
    testFrame.getContentPane().add(testPanel);

    testFrame.setSize(540, 200);
    testFrame.setVisible(true);
  }
}
