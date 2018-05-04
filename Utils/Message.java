package Utils;

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

import java.awt.*;
import java.util.*;
import javax.swing.*;
import java.awt.event.*;

public class Message
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";



  public static void abortMsg(String text)
  {
    JOptionPane.showMessageDialog(null, text + "\n",
                                  "Processing Aborted",
                                  JOptionPane.ERROR_MESSAGE);

    throw new IllegalArgumentException(text);
  }


  public static void abortMsg(String text, String title)
  {
    JOptionPane.showMessageDialog(null, text + "\n",
                                  "Processing Aborted",
                                  JOptionPane.ERROR_MESSAGE);

    throw new IllegalArgumentException(text);
  }


  public static boolean infoMsg(String text)
  {
    JOptionPane.showMessageDialog(null, text,
                                  "Information message",
                                  JOptionPane.INFORMATION_MESSAGE);

    /* This 'false' is returned to allow one-line displays and returns by callers */
    return false;
  }


  public static boolean askOkCancel(String text)
  {
    int rc = JOptionPane.showConfirmDialog(null, text,
                                           "Information message",
                                           JOptionPane.OK_CANCEL_OPTION);

    return rc == JOptionPane.OK_OPTION;
  }



  /**
   * Centers the frame
   */
  public static void centerscreen(Container container)
  {
    Dimension dim = container.getToolkit().getScreenSize();
    Rectangle abounds = container.getBounds();
    container.setLocation((dim.width - abounds.width) / 2,
                          (dim.height - abounds.height) / 2);
    container.requestFocus();
  }

  /**
   * slowly allow new frames to be created in a slightly different position
   */
  private static int xy_location = 0;
  public static void set_location(Container container)
  {
    xy_location += 20;
    container.setLocation(xy_location, xy_location);
  }

}

