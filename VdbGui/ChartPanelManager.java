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

import java.awt.*;
import javax.swing.*;
import Vdb.Gui_perf_data;
import Vdb.GuiServer;
import Vdb.common;


public class ChartPanelManager
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // Indices to refer to each chart managed.
  public final static int IO_CHART = 0;
  public final static int RT_CHART = 1;
  public final static int DR_CHART = 2;

  // Necessary conversion factors
  public final static double MS_PER_SEC = 1000;
  public final static double BYTES_PER_MB = 1024 * 1024;

  private GuiServer client;

  private JButton displayChartsButton;

  /**
   */
  public ChartPanelManager(JButton displayButton)
  {
    displayChartsButton = displayButton;
  }


  /**
   *
   */
  public void closeServerSocket()
  {
    if (client != null)
    {
      client.closeSocket();
    }
  }

  /**
   * Processes the data from VDBench and sends it to each chart for display.
   */
  public void processData()
  {
    // Initialize the DateLineChart objects and the corresponding DataSet objects.
    initializeCharts();

    // Connect to server.
    //GuiServer client = new GuiServer();
    if (client == null)
    {
      client = new GuiServer();
      client.connect_to_client(ButtonPanel.getPortNumber());
    }

    // Acquire performance data for display.
    while (client.get_message())
    {
      // Tell vdbench that we received the message:
      //
      client.put_message("Acknowledge");

      // Pick up the message and send it to the chart:
      Vdb.Gui_perf_data gp = (Vdb.Gui_perf_data)client.get_data();
      if (gp == null)
      {
        return;
      }

      // Acquire the values needed for display.
      double iops        = gp.iops;
      double respTime    = gp.resp_time;
      double dataRate    = gp.megabytes;

      // And add them to the appropriate data sets.
      long currentTime = gp.end_ts.getTime();

      if (!displayChartsButton.isEnabled())
      {
        displayChartsButton.setEnabled(true);
      }
    }
  }

  // Set the chart titles and create DataSet objects to subsequently store
  // data for each chart.
  private void initializeCharts()
  {
  }
}
