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
 * <p>Title: AboutDialog.java</p>
 * <p>Description: A dialog class which displays simple information.</p>
 * @author Jeff Shafer
 * @version 1.0
 */

import javax.swing.*;
import java.awt.event.*;
import java.awt.*;

public class AboutDialog extends JDialog implements ActionListener
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";


  private JPanel buttonPanel;
  private JButton okButton;
  private JButton cancelButton;
  private boolean isApproved = false;

  /**
   * Constructor which takes the parent application with which the dialog is associated,
   * and strings for the dialog title and messages.
   * @param parent the application on behalf of which the dialog appears.
   * @param title the dialog title.
   * @param messages the dialog messages.
   */
  public AboutDialog(Frame parent, String title, String[] messages)
  {
    super(parent, title, true);

    // If a parent exists, set the dialog inside it.
    if(parent != null)
    {
      Dimension parentDimension = parent.getSize();
      Point p = parent.getLocation();
      setLocation(p.x + parentDimension.width/4, p.y + parentDimension.height/4);
    }

    // Create the main panel.
    JPanel panel = new JPanel();
    panel.setLayout(new GridLayout(messages.length, 1, 8, 8));
    for(int i = 0; i < messages.length; i++)
    {
      panel.add(new JLabel(messages[i]));
    }
    getContentPane().add(panel);

    // Create the button panel and add the button.
    buttonPanel = new JPanel();
    okButton = new JButton("OK");
    buttonPanel.add(okButton);
    okButton.addActionListener(this);
    getContentPane().add(buttonPanel, BorderLayout.SOUTH);

    // Set the close operation, pack the frame, and make it visible.
    setDefaultCloseOperation(DISPOSE_ON_CLOSE);
    pack();
    setVisible(false);
    setResizable(false);
  }

  public boolean isApproved()
  {
    return isApproved;
  }

  public void setDisapproval()
  {
    isApproved = false;
  }

  /**
   * Explicitly makes this panel visible.
   */
  //public void setVisible()
  {
    //setVisible();
  }

  /**
   * Allows the caller to add a cancel button to the dialog if desired,
   * with the listener added as an argument.
   * @param aListener the listener for the cancel button.
   */
  public void addCancelButtonAndListener(ActionListener aListener)
  {
    cancelButton = new JButton("Cancel");
    buttonPanel.add(cancelButton);
    cancelButton.addActionListener(aListener);
    pack();
  }

  // Perform the button action.
  public void actionPerformed(ActionEvent e)
  {
    // Make the dialog invisible and get rid of it.
    isApproved = true;
    setVisible(false);
    dispose();
  }
}
