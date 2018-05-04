package VdbComp;

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
import java.awt.event.*;
import javax.swing.*;
import Utils.common;
import Utils.Format;

/**
 * Allow customer to modify the percentage ranges
 */
public class ChangeRanges extends JDialog implements ActionListener
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private JButton ok_button = new JButton("OK");
  private JButton cancel    = new JButton("Cancel");

  private Delta[] deltas = Delta.getDeltas();
  private JTextField[] txt_fields = new JTextField[deltas.length];
  private WlComp wlcomp = null;


  public ChangeRanges(WlComp wl)
  {
    setModal(true);
    wlcomp = wl;
    getContentPane().setLayout(new GridLayout(deltas.length + 1, 2));

    for (int i = 0; i < deltas.length; i++)
    {
      Delta delta = deltas[i];
      txt_fields[i] = new JTextField();
      txt_fields[i].setHorizontalAlignment(JTextField.RIGHT);
      txt_fields[i].setText("" + delta.limit);

      /* The Delta instance may not be active in two places at the time. */
      /* I therefore must duplicate the JTextField:                      */
      JTextField tf = new JTextField(); //delta.range_label);
      tf.setBackground(delta.color);
      tf.setHorizontalAlignment(JTextField.CENTER);
      tf.setEditable(false);
      getContentPane().add(tf);
      getContentPane().add(txt_fields[i]);

      if (delta.limit == 0)
        txt_fields[i].setEnabled(false);
    }

    getContentPane().add(ok_button);
    getContentPane().add(cancel);

    ok_button.addActionListener(this);
    cancel.addActionListener(this);

  }

  public static void main(String[] args)
  {
    /*
    ChangeRanges cr = new ChangeRanges();

    cr.addWindowListener(new WindowAdapter()
                      {
                        public void windowClosing(WindowEvent e)
                        {
                          System.exit(0);
                        }
                      });

    cr.setLocation(120, 120);

    cr.pack();
    cr.setVisible(true);
    */
  }


  public void actionPerformed(ActionEvent e)
  {
    String cmd = e.getActionCommand();

    if (cmd.equals(ok_button.getText()))
    {
      for (int i = 1; i < txt_fields.length; i++)
      {
        if (Double.parseDouble(txt_fields[i].getText()) >=
            Double.parseDouble(txt_fields[i-1].getText()))
        {
          JOptionPane.showMessageDialog(this,
                                        "All percentages entered must decrement in value",
                                        "Try again",
                                        JOptionPane.ERROR_MESSAGE);
          return;
        }
      }


      double[] new_deltas = new double[txt_fields.length];
      Delta[]  old_deltas = Delta.getDeltas();
      for (int i = 0; i < txt_fields.length; i++)
        new_deltas[i] = Double.parseDouble(txt_fields[i].getText());

      Delta.setDeltas(new_deltas);
      //wlcomp.invalidate();
      //wlcomp.update(getGraphics());

      if (wlcomp.table_panel != null)
        wlcomp.doCompare();

      this.dispose();
    }

    else if (cmd.equals(cancel.getText()))
      this.dispose();
  }

}
