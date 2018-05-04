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
 * <p>Title: PrintablePanel.java</p>
 * <p>Description: This panel extends JPanel to implement the Printable
 * interface and allow its contents to be printed.</p>
 * @author Jeff Shafer
 * @version 1.0
 */

import javax.swing.*;
import java.awt.*;
import java.awt.print.*;

public class PrintablePanel extends JPanel implements Printable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  /**
   * This is the implementation of interface <code>Printable</code>; it allows the
   * printing of a <code>ChartPanel</code> object.
   * @param g A <code>Graphics</code> object, which is cast to a <code>Graphics2D</code> object and
   *          used to manipulate and format the printable area.
   * @param pageFormat Object describing the printed sheet and its orientation.
   * @param pageIndex The number of the page printed; it starts at zero.
   * @return A constant in the <code>Printable</code> interface.  If the page does not
   *         exist, then <code>Printable.NO_SUCH_PAGE</code> is returned.
   *         If the page is exists and is able to be printed,
   *         returns <code>Printable.PAGE_EXISTS</code>.
   */
  public int print(Graphics g, PageFormat pageFormat, int pageIndex)
  {
    // We only want to print one page, so the page index should only be zero.
    if(pageIndex > 0)
    {
      return Printable.NO_SUCH_PAGE;
    }

    Graphics2D g2d = (Graphics2D)g;

    // Translate to the origin (upper left corner) of the printable area of the page.
    g2d.translate(pageFormat.getImageableX(), pageFormat.getImageableY());

    // Disable double buffering so that we can print at maximum printer resolution,
    // rather than being limited to the resolution permitted by double buffering.
    boolean wasBuffered = disableDoubleBuffering(this);

    // Call paint to allow the panel to be printed, and restore its double buffering
    // for flicker-free display.
    paint(g2d);
    restoreDoubleBuffering(this, wasBuffered);

    // Inform the caller that we were able to print the page.
    return Printable.PAGE_EXISTS;
  }

  // Disables double buffering to ensure that when when printing,
  // the image resolution is not limited to the low resolution
  // of the the off-screen buffer, (typically 72 dpi).
  //
  // Input: jc, a JComponent.
  // Output: A boolean indicating if the component was double buffered.
  private boolean disableDoubleBuffering(JComponent jc)
  {
    boolean wasBuffered = jc.isDoubleBuffered();
    jc.setDoubleBuffered(false);
    return wasBuffered;
  }

  // Re-enables double buffering for a JComponent so that it may be drawn
  // onscreen without flickering.
  // Input: jc, a JComponent.
  // Output: A boolean value indicating if that JComponent was double buffered.
  private void restoreDoubleBuffering(JComponent jc, boolean wasBuffered)
  {
    jc.setDoubleBuffered(wasBuffered);
  }
}
