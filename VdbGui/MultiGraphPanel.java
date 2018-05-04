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
 * Code left after the removal of the performance chart
 */
import javax.swing.*;
import javax.swing.border.*;
import java.awt.*;
import java.awt.event.*;
import java.awt.print.*;

public class MultiGraphPanel extends JFrame implements Runnable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // Panel to fit in the frame and hold all its contents.
  private  JPanel contentPanel = new JPanel();

  // Panel to display individual chart panels.
  private  JPanel displayPanel = new JPanel();

  // Panel to hold the "close" button.
  private JPanel buttonPanel  = new JPanel();

  // Manages all the charts.
  ChartPanelManager chartManager;

  private JButton chartDisplayButton;

  /**
   * No-argument constructor.
   */
  public MultiGraphPanel()
  {
    // Set the frame title.
    super("Vdbench 5.01 - Activity Charts");

    // Set the border and layout for the frame contents.
    contentPanel.setBorder(new TitledBorder("Real-Time Activity Charts"));
    contentPanel.setLayout(new BorderLayout());

    // Build display area.
    buildDisplayArea();

    // Add display area and button to close panel.
    contentPanel.add(displayPanel, BorderLayout.CENTER);

    contentPanel.add(buttonPanel, BorderLayout.SOUTH);
    getContentPane().add(contentPanel);

    // Set the frame size, make it visible, and be sure that it shuts down properly.
    setSize(750, 500);
    //show();
    setDefaultCloseOperation(JFrame.HIDE_ON_CLOSE);
  }

  /**
   * Implements run method to start server socket in separate thread.
   */
  public void run()
  {
    startCharts();
  }

  /**
   * Provides the application with a reference to the button used to display charts.
   * This allows the button to be activated only after charts have been created, thereby
   * preventing the user from accessing this functionality when the required data structures
   * have not yet been built.
   * @param displayButton a reference to the button object which allows the user to display charts.
   */
  public void setDisplayButton(JButton displayButton)
  {
    chartDisplayButton = displayButton;
  }

  /**
   * Sends a message back to the ChartManager object telling it to close the
   * server socket.  This prevents a conflict with socket binding if the user
   * interrupts the current run and then decides to start a new run.
   */
  public void closeSocket()
  {
    if(chartManager != null)
    {
      chartManager.closeServerSocket();
    }
  }

  /**
   * Starts a separate thread of execution to acquire chart data from VDBench.
   */
  public void start()
  {
    Thread runner = new Thread(this);
    runner.start();
  }


  /**
   * Tell the various chart panels to begin diaplaying data from vdbench.
   */
  public void startCharts()
  {
    chartManager.processData();
  }

  public void renewCharts()
  {
    displayPanel.removeAll();
    chartManager = null;

    // Create a chart panel manager to create and manage the charts.
    chartManager = new ChartPanelManager(chartDisplayButton);
  }

  // Adds graph panels to display area; three rows, one column.
  private void buildDisplayArea()
  {
    displayPanel.setLayout(new GridLayout(3, 1, 5, 10));
  }

  // A short main to test.
  public static void main(String [] args)
  {
    MultiGraphPanel activityPanel = new MultiGraphPanel();
  }
}
