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
 * This class handles the matching of delta percentages with the colors
 * in which they are displayed in the JTable.
 */
public class Delta extends JTextField
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  double limit;
  String range_label;
  Color  color;


  private static Delta[] deltas = new Delta[]
  {
    new Delta(+4.,  new Color( 51,  153, 0   )),
    new Delta(+3.,  new Color( 102, 204, 0   )),
    new Delta(+2.,  new Color( 153, 255, 0   )),
    new Delta(+1.,  new Color( 204, 255, 51  )),
    new Delta(0.,   new Color( 255, 255, 255 )),
    new Delta(-1.,  new Color( 255, 255, 0   )),
    new Delta(-2.,  new Color( 255, 204, 0   )),
    new Delta(-3.,  new Color( 255, 153, 0   )),
    new Delta(-4.,  new Color( 255, 0,   0   ))
  };

  static
  {
    createLabels();
  };

  private static Dimension dim = new Dimension(40,26);




  public Delta(double limit, Color color)
  {
    this.limit = limit;
    this.color = color;

    setBackground(color);
    setMinimumSize(dim);
    setPreferredSize(dim);
    setEditable(false);
    setHorizontalAlignment(JTextField.CENTER);
  }


  /**
   * Use the range limits to create printable labels.
   */
  private static void createLabels()
  {
    for (int i = 0; i < deltas.length; i++)
    {
      Delta delta = deltas[i];
      if (i == 0)
        delta.range_label = "+" + delta.limit + "%>";

      else if (i == deltas.length - 1)
        delta.range_label = delta.limit + "%<";

      else if (delta.limit > 0)
        delta.range_label = "+" + delta.limit + "%";

      else
        delta.range_label = "" + delta.limit + "%";

      delta.setText(delta.range_label);
    }
  }


  public static Delta[] getDeltas()
  {
    return deltas;
  }
  public static void setDeltas(double[] dlts)
  {
    for (int i = 0; i < dlts.length; i++)
      deltas[i].limit  = dlts[i];

    createLabels();
  }


  public static void main(String[] args)
  {
    double[] pcts   = new double[] {  4,    3,   2,   1,  0,  -1,  -2,  -3,  -4};
    String[] labels = new String[] {"+4>","+3","+2","+1","0","-1","-2","-3","-4<",};

    for (int i = 0; i < args.length; i++)
    {
      double pct = Double.parseDouble(args[i]);

      boolean found = false;
      for (int j = 0; j < pcts.length; j++)
      {
        if (pct >= pcts[j])
        {
          common.ptod("match1: " + pct + " " + labels[j]);
          found = true;
          break;
        }
      }

      if (!found)
        common.ptod("match3: " + pct + " " + labels[labels.length - 1]);
    }
  }
}


class DeltaValue
{
  String delta_value;
  Color  color;

  /**
   * Create info we need to determine the contents and color of a delta value.
   */
  public DeltaValue(double value)
  {
    if (value < 0)
      delta_value = " " + Format.f("%.1f%%",value);

    else if (value == 0)
      delta_value = " " + Format.f("%.1f%%",value);

    else
      delta_value = "+" + Format.f("%.1f%%",value);

    Delta[] deltas = Delta.getDeltas();
    for (int i = 0; i < deltas.length; i++)
    {
      Delta delta = deltas[i];

      if (value >= delta.limit)
      {
        if (value >= 0)
          color = delta.color;
        else
          color = deltas[i-1].color;
        return;
      }
    }

    color = deltas[deltas.length - 1].color;
  }
}
