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
 * <p>Title: RunParameterPanel.java</p>
 * <p>Description: This class displays the following run input values:
 * thread count, run duration in seconds, and I/O rate per second.</p>
 * @author Jeff Shafer
 * @version 1.0
 * Date: 5/03/04
 */

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import java.util.StringTokenizer;
import java.util.Vector;

public class RunParameterPanel extends JPanel
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // The number of fields (and labels) in the panel.
  private static final int FIELD_NUMBER = 6;

  // Text fields for the entries.
  private JTextField threadField   = new JTextField("8", 8);
  private JTextField durationField = new JTextField("30", 8);
  private JTextField ioRateField   = new JTextField("100", 8);

  // Labels corresponding to the text fields.
  private JLabel threadLabel   = new JLabel("Number of Threads");
  private JLabel durationLabel = new JLabel("Duration (secs)");
  private JLabel ioRateLabel   = new JLabel("I/O Rate (per sec)");

  // Members to hold values entered in the corresponding fields.
  private int[] numberOfThreads;
  private int duration;
  private String ioRate;
  private String entry = null;

  //private RunParmFocusListener threadListener = new RunParmFocusListener(threadField, "thread");
  //private RunParmFocusListener durationListener = new RunParmFocusListener(durationField, "duration");
  //private RunParmFocusListener ioListener = new RunParmFocusListener(ioRateField, "io");

  //private int durationValue;
  //private String ioValue;
  //private String threadValue;

  // Subpanels to hold labels and fields so they may be properly arranged in
  // the panel.
  private JPanel[] fieldPanel = new JPanel[FIELD_NUMBER];

  /**
   * No-argument constructor.
   */
  public RunParameterPanel()
  {
    // Set the layout to three rows and two columns.
    setLayout(new GridLayout(3, 2, 5, 5));

    // Build the fields for each entry.
    buildFields();

    // Add tool tips.
    setToolTips();

    // Add the focus listeners
    //addFocusListener();
    //threadField.addFocusListener(threadListener);
    //durationField.addFocusListener(durationListener);
    //ioRateField.addFocusListener(ioListener);
  }

  public void setEditable(boolean isEditable)
  {
    durationField.setEditable(isEditable);
    threadField.setEditable(isEditable);
    ioRateField.setEditable(isEditable);
  }

  /**
   * Performs error checking on duration and I/O rate run parameters
   * @return an array of strings containing a run definition for each
   * I/O rate value.
   */
  public boolean areRunParmsValid()
  {
    //removeFocusListener();

    // Get values as Strings from fields.
    String durationString = durationField.getText();
    String ioRateString   = ioRateField.getText();
    String threadString   = threadField.getText();

    // Validate the entries.
    int durationValue  = 0;
    String ioValue     = "";
    String threadValue = "";

    boolean isValid = false;

    try
    {
      durationValue = validateDuration(durationString);
      ioValue = validateIOEntry(ioRateString);
      threadValue = validateThreadCount(threadString);
      isValid = true;
    }
    catch (Exception e)
    {
      JOptionPane.showMessageDialog(VDBenchGUI.getGUIFrame(), e.getMessage(),
                                    "Invalid Run Parameter",
                                    JOptionPane.ERROR_MESSAGE);
      e.printStackTrace();
    }
    finally
    {
      //addFocusListener();
    }

    entry = ",iorate=" + ioValue + ",elapsed=" + durationValue + ",forthreads=(" + threadValue + ")";
    return isValid;
  }

  public String getEntries()
  {
    return entry;
  }

  /**
   * Provides thread count as an integer.
   * @return the number of threads selected by the user at the time the run
   * button is pressed.
   */
  /*public String getThreadCount()
  {
    String threadString = threadField.getText();
    return validateThreadCount(threadString);
  }*/

  /**
   * Sets the fields in this panel when a file has been opened and its values
   * extracted for display
   * @param threads the number of threads for this run.
   * @param duration the run duration in seconds.
   * @param ioRate the I/O rate, per second.
   */
  public void setFields(String threads, String duration, String ioRate)
  {
    threadField.setText(threads);
    durationField.setText(duration);
    ioRateField.setText(ioRate);
  }

  /**
   * Starts listening for focus events for the thread, duration, and I/O rate fields.
   */
  /*public void addFocusListener()
  {
    threadField.addFocusListener(threadListener);
    durationField.addFocusListener(durationListener);
    ioRateField.addFocusListener(ioListener);
  }*/

  /**
   * Stops listening for focus events for the thread, duration, and I/O rate fields.
   */
  /*public void removeFocusListener()
  {
    threadField.removeFocusListener(threadListener);
    durationField.removeFocusListener(durationListener);
    ioRateField.removeFocusListener(ioListener);
  }*/

  // Places each entry component, either label or text field,
  // in its own panel for proper layout.
  private void buildFields()
  {
    // Create the anonymous subpanels.
    for (int i = 0; i < FIELD_NUMBER; i++)
    {
      fieldPanel[i] = new JPanel();
    }

    // Add fields and labels.
    fieldPanel[0].add(threadLabel);
    fieldPanel[1].add(threadField);
    fieldPanel[2].add(durationLabel);
    fieldPanel[3].add(durationField);
    fieldPanel[4].add(ioRateLabel);
    fieldPanel[5].add(ioRateField);

    // Add the subpanels to the main panel.
    for (int i = 0; i < FIELD_NUMBER; i++)
    {
      add(fieldPanel[i]);
    }
  }

  // Set the tool tip text.
  private void setToolTips()
  {
    threadField.setToolTipText("Enter the desired number of concurrent I/Os.");
    durationField.setToolTipText("Enter the desired duration of each workload run, in seconds.");
    ioRateField.setToolTipText("Enter the desired I/O rate(s), per second.  Note that in addition to a comma-separated list of values, one may also enter the words \"Curve\" or \"Max\"");
  }

  // Validates thread count.
  private String validateThreadCount(String threadCount)
  {
    StringTokenizer st = new StringTokenizer(threadCount, ", \n\r\f");
    int numberOfTokens = st.countTokens();

    // Make sure that we have at least one thread count value.
    if(numberOfTokens == 0)
    {
      throw new IllegalArgumentException("Please enter at least one thread count value.");
    }

    String displayString = "";
    int value;

    // Get the entries, and examine each in turn.
    for(int i = 0; i < numberOfTokens; i++)
    {
      // If we have multiple arguments, separate them in the display string using commas.
      if(i > 0)
      {
        displayString += ",";
      }

      String s = st.nextToken();
      try
      {
        value = Integer.parseInt(s);

        // Make sure that the value is in range.
        if (value < 1 || value > 256)
        {
          throw new IllegalArgumentException("Thread count values must be > 0 and <= 256.");
        }
      }
      catch (NumberFormatException nfe)
      {
        // This replaces the original NumberFormatException thrown by "parseInt".
        throw new NumberFormatException("Non-numeric value in thread count field.");
      }
      displayString += Integer.toString(value);
    }
    threadField.setText(displayString);
    return displayString;
  }

  // Validates thread count.
  /*private int validateThreadCount(String threadCount)
  {
    StringTokenizer st = new StringTokenizer(threadCount, ", \t\n\r\f");
    int numberOfTokens = st.countTokens();

    // Make sure that we have only one thread count value.
    if(numberOfTokens == 0 || numberOfTokens > 1)
    {
      throw new IllegalArgumentException("Please enter a single thread count value.");
    }

    String s = st.nextToken();
    int value = 0;

    try
    {
      // Parse the value; if it is not a valid integer, a number format exception will
      // be thrown.  This will be caught below, and replaced with an exception of the
      // same type, but with a more meaningful message.  This is then thrown to the caller
      // of this function.
      value = Integer.parseInt(s);

      // Make sure that the value is in range.  If not, tell the user.
      if(value < 1 || value > 256)
      {
        throw new IllegalArgumentException("Thread count must be > 0 and <= 256.");
      }
    }
    catch(NumberFormatException nfe)
    {
      // This replaces the original NumberFormatException thrown by "parseInt".
      throw new NumberFormatException("Non-numeric value in thread field.");
    }
    return value;
  }*/


  // Validates duration interval.
  private int validateDuration(String duration)
  {
    StringTokenizer st = new StringTokenizer(duration, ", \n\r\f");
    int numberOfTokens = st.countTokens();

    // Make sure that we have only one duration value.
    if(numberOfTokens == 0 || numberOfTokens > 1)
    {
      throw new IllegalArgumentException("Please enter a single duration value.");
    }

    String s = st.nextToken();
    int value = 0;

    try
    {
      value = Integer.parseInt(s);

      // Make sure that the value is in range.
      if(value < 1)
      {
        throw new IllegalArgumentException("Duration must be > 0 seconds.");
      }
    }
    catch(NumberFormatException nfe)
    {
      // This replaces the original NumberFormatException thrown by "parseInt".
      throw new NumberFormatException("Non-numeric value in duration field.");
    }
    return value;
  }

  // Validates I/O entries.
  private String validateIOEntry(String ioEntries)
  {
    StringTokenizer st = new StringTokenizer(ioEntries, ", \n\r\f");
    int numberOfTokens = st.countTokens();

    // Check to make sure we have only one value or keyword.
    if(numberOfTokens == 0 || numberOfTokens > 1)
    {
      throw new IllegalArgumentException("Please enter a single I/O rate value or keyword.");
    }

    String s = st.nextToken();
    String displayString = "";

    int value;

    // If the value of the token is one of the keywords "max" or
    // "curve", accept it...
    if(s.trim().equalsIgnoreCase("max") || s.trim().equalsIgnoreCase("curve"))
    {
      displayString = s.trim().toLowerCase();
      ioRateField.setText(displayString);
      return displayString;
    }

    try
    {
      value = Integer.parseInt(s);

      // Make sure that the value is in range.
      if (value < 0)
      {
        throw new IllegalArgumentException("I/O rate must be an integer > 0.");
      }
    }
    catch (NumberFormatException nfe)
    {
      // This replaces the original NumberFormatException thrown by "parseInt".
      throw new NumberFormatException("Illegal non-numeric value in I/O rate field.");
    }
    displayString = Integer.toString(value);

    ioRateField.setText(displayString);
    return displayString;
  }

  // Validates I/O entries.
  /*private String[] validateIOEntries(String ioEntries)
  {
    StringTokenizer st = new StringTokenizer(ioEntries, ", \t\n\r\f");
    int numberOfTokens = st.countTokens();

    // Check to make sure we have only one value or keyword.
    if(numberOfTokens == 0 || numberOfTokens > 1)
    {
      throw new IllegalArgumentException("Please enter a single I/O rate value or keyword.");
    }

    String[] valueArray = new String[numberOfTokens];
    String displayString = "";

    int value;

    // Get the entries, and examine each in turn.
    for(int i = 0; i < numberOfTokens; i++)
    {
      // If we have multiple arguments, separate them in the display string using commas.
      if(i > 0)
      {
        displayString += ",";
      }

      String s = st.nextToken();

      // If the value of the token is one of the keywords "max" or
      // "curve", accept it...
      if(s.trim().equalsIgnoreCase("max") || s.trim().equalsIgnoreCase("curve"))
      {
        // ...if it is the first and only argument in this field.
        if(i == 0 && numberOfTokens == 1)
        {
          displayString = s.trim().toUpperCase();
          valueArray[0] = displayString;
          break;
        }
        // Otherwise, let the user know he has made an error.
        throw new IllegalArgumentException("I/O rate keywords must be used alone.");
      }
      try
      {
        value = Integer.parseInt(s);

        // Make sure that the value is in range.
        if (value < 0)
        {
          throw new IllegalArgumentException("I/O rate must be an integer > 0.");
        }
      }
      catch (NumberFormatException nfe)
      {
        // This replaces the original NumberFormatException thrown by "parseInt".
        throw new NumberFormatException("Illegal non-numeric value in I/O rate field.");
      }
      valueArray[i] = Integer.toString(value);
      displayString += value;
    }
    ioRateField.setText(displayString);
    return valueArray;
  }*/

  // A short main to test.
  public static void main(String [] args)
  {
    JFrame frame = new JFrame();
    RunParameterPanel panel = new RunParameterPanel();
    frame.getContentPane().add(panel);

    frame.setSize(300, 300);
    frame.setVisible(true);
  }

  private class RunParmFocusListener implements FocusListener
  {
    private JTextField jComp;
    private String name;

    RunParmFocusListener(JTextField jc, String n)
    {
      jComp = jc;
      name = n;
    }

    public void focusGained(FocusEvent e)
    {

    }

    // We must update all run definitions.
    public void focusLost(FocusEvent e)
    {

      jComp.removeFocusListener(this);
      // Check for errors first.
      String postString = null;
      boolean validThread = false;
      boolean validDuration = false;
      boolean validIO = false;

      if(name == "thread")
      {
        try
        {
          System.out.println(name);
          validateThreadCount(jComp.getText());
          validThread = true;
          if(!durationField.isEditable())
          {
            durationField.setEditable(true);
          }
          if(!ioRateField.isEditable())
          {
            ioRateField.setEditable(true);
          }
        }
        catch(Exception ex)
        {
          durationField.setEditable(false);
          ioRateField.setEditable(false);
          JOptionPane.showMessageDialog(VDBenchGUI.getGUIFrame(), ex.getMessage(), "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
          return;

        }
        finally
        {
          jComp.addFocusListener(this);
        }
      }
      if(name == "duration")
      {
        try
        {
          System.out.println(name);
          validateDuration(jComp.getText());
          validDuration = true;
          if(!threadField.isEditable())
          {
            threadField.setEditable(true);
          }
          if(!ioRateField.isEditable())
          {
            ioRateField.setEditable(true);
          }
        }
        catch(Exception ex)
        {
          threadField.setEditable(false);
          ioRateField.setEditable(false);
          JOptionPane.showMessageDialog(VDBenchGUI.getGUIFrame(), ex.getMessage(), "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
          return;

        }
        finally
        {
          jComp.addFocusListener(this);
        }
      }
      if(name == "io")
      {
        try
        {
          System.out.println(name);
          validateIOEntry(jComp.getText());
          validIO = true;
          if(!durationField.isEditable())
          {
            durationField.setEditable(true);
          }
          if(!threadField.isEditable())
          {
            threadField.setEditable(true);
          }
        }
        catch(Exception ex)
        {
          durationField.setEditable(false);
          threadField.setEditable(false);
          JOptionPane.showMessageDialog(VDBenchGUI.getGUIFrame(), ex.getMessage(), "Invalid Run Parameter", JOptionPane.ERROR_MESSAGE);
          return;
        }
        finally
        {
          jComp.addFocusListener(this);
        }
      }
      postString = getEntries();

      // Examine each run definition and change it.
      Vector runs = VDBenchGUI.getRunDefinitions();

      // Now, look at each run, and edit that portion of it which corresponds
      // to the field which may have just changed.
      for(int i = 0; i < runs.size(); i++)
      {
        String runString = (String)runs.get(i);
        if(runString == null || runString.length() == 0)
        {
          return;
        }
        //System.out.println("Run[" + i + "]:" + runString);

        // Get the first part of the string.
        String preString = runString.substring(runString.indexOf("rd="), runString.indexOf(",iorate="));

        // Get the iorate substring.
        String subString = runString.substring(runString.indexOf("iorate="));
        String ioRateString = subString.substring(subString.indexOf("iorate=") + "iorate=".length(), subString.indexOf(","));
        //System.out.println("ioRateString: " + ioRateString);

        subString = subString.substring(subString.indexOf("elapsed="));
        String durationString = subString.substring(subString.indexOf("elapsed=") + "elapsed=".length(), subString.indexOf(","));
        //System.out.println("durationString: " + durationString);

        subString = subString.substring(subString.indexOf("forthreads=("));
        String threadString = subString.substring(subString.indexOf("forthreads=(") + "forthreads=(".length(), subString.indexOf(")"));
        //System.out.println("threadString: " + threadString);

        int xferIndex = subString.indexOf("forxfersize=(");
        String xferString = "";
        if(xferIndex != -1)
        {
          subString = subString.substring(subString.indexOf("forxfersize=("));
          xferString = subString.substring(subString.indexOf("forxfersize=("), subString.indexOf(")") + 1);
        }

        // Set the location of the focus event.
        /*if((JTextField)e.getSource() == threadField)
        {
          threadString = "forthreads=(" + threadString + ")";
          durationString = "elapsed=" + durationString + ",";
          ioRateString = "iorate=" + ioRateString + ",";
        }
        if((JTextField)e.getSource() == durationField)
        {
          threadString = "forthreads=(" + threadString + ")";
          durationString = "elapsed=" + durationString + ",";
          ioRateString = "iorate=" + ioRateString + ",";
        }
        if((JTextField)e.getSource() == ioRateField)
        {
          threadString = "forthreads=(" + threadString + ")";
          durationString = "elapsed=" + durationString + ",";
          ioRateString = "iorate=" + ioRateString + ",";
        }*/

        if(xferIndex != -1)
        {
          //threadString += ",";

          //threadString += xferString;
          postString += "," + xferString;
        }

        String outputString = preString + postString;//ioRateString + durationString + threadString;

        runs.setElementAt(outputString, i);
      }
      VDBenchGUI.getMonitorPanel().writeEntriesToPanel(VDBenchGUI.getWorkloadDefinitions(), runs);
    }
  }
}
