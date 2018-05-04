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
 * <p>Title: VDBWorkload.java</p>
 * <p>Description: This container class holds workload specific parameters.  It is used to represent
 * a workload in the drop-down list of predefined workloads displayed on the GUI.</p>
 * @author Jeff Shafer
 * @version 1.0
 */
import javax.swing.*;
import java.util.StringTokenizer;

public class VDBWorkload
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  // Specific workload parameters.
  private String workloadName;
  private int writeHitPct;
  private int readPct;
  private int readHitPct;
  private int randomPct;
  private String xferSize;

  /**
   * Constructs a VDBench basic workload object.
   * @param name name of the workload.
   * @param whPct write hit percentage as a value between 0 and 100, inclusive.
   * @param rPct read percentage as a value between 0 and 100, inclusive.
   * @param rhPct read hit percentage as a value between 0 and 100, inclusive.
   * @param randPct random percentage as a value between 0 and 100, inclusive.
   * @param xferSz transfer size in kilobytes.
   */
  public VDBWorkload(String name, int whPct, int rPct, int rhPct, int randPct, String xferSz)
  {
    workloadName = name;
    writeHitPct  = whPct;
    readPct      = rPct;
    readHitPct   = rhPct;
    randomPct    = randPct;
    xferSize = xferSz;
  }

  /**
   * Constructs a VDBench basic workload object using a line from a file.
   * @param inputLine a line of comma delimited text from the file workloadDefinitions.txt.
   */
  public VDBWorkload(String inputLine)
  {
    StringTokenizer st = new StringTokenizer(inputLine, ",");
    workloadName  = st.nextToken();
    try
    {
      writeHitPct  = Integer.parseInt(st.nextToken());
      readPct      = Integer.parseInt(st.nextToken());
      readHitPct   = Integer.parseInt(st.nextToken());
      randomPct    = Integer.parseInt(st.nextToken());
      xferSize     = st.nextToken();
    }
    catch(NumberFormatException nfe)
    {
      // If any of the above tokens are not parsable as integers, let the user know.
      JOptionPane.showMessageDialog(null, "Workload improperly specified in input file.", "Error Reading From File", JOptionPane.ERROR_MESSAGE);
      nfe.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Overrides <code>toString()</code> in <code>Object</code> and provides the
   * name of the workload.
   * @return the VDBWorkload name.
   */
  public String toString()
  {
    return workloadName;
  }

  /**
   * Provides workload write hit percentage.
   * @return write hit percentage as a value between 0 and 100, inclusive.
   */
  public int getWriteHitPct()
  {
    return writeHitPct;
  }

  /**
   * Provides workload read percentage.
   * @return read percentage as a value between 0 and 100, inclusive.
   */
  public int getReadPct()
  {
    return readPct;
  }

  /**
   * Provides workload read hit percentage.
   * @return read hit percentage as a value between 0 and 100, inclusive.
   */
  public int getReadHitPct()
  {
    return readHitPct;
  }

  /**
   * Provides random percentage.
   * @return random percentage as a value between 0 and 100, inclusive.
   */
  public int getRandomPct()
  {
    return randomPct;
  }

  /**
   * Provides transfer size.
   * @return transfer size .
   */
  public String getxferSize()
  {
    return xferSize;
  }
}
