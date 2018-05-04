package VdbComp;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
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
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  double limit;
  String range_label;
  Color  color;


  private static Delta[] deltas = null;

  static
  {
    setDefaults();
    createLabels();
  };

  private static Dimension dim = new Dimension(40,26);



  public Delta()
  {
  }

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


  public static void setDefaults()
  {
    if (deltas == null)
    {
      deltas    = new Delta[9];
      deltas[0] = new Delta(+4.,  new Color( 51,  153, 0   ));
      deltas[1] = new Delta(+3.,  new Color( 102, 204, 0   ));
      deltas[2] = new Delta(+2.,  new Color( 153, 255, 0   ));
      deltas[3] = new Delta(+1.,  new Color( 204, 255, 51  ));
      deltas[4] = new Delta(0.,   new Color( 255, 255, 255 ));
      deltas[5] = new Delta(-1.,  new Color( 255, 255, 0   ));
      deltas[6] = new Delta(-2.,  new Color( 255, 204, 0   ));
      deltas[7] = new Delta(-3.,  new Color( 255, 153, 0   ));
      deltas[8] = new Delta(-4.,  new Color( 255, 0,   0   ));
    }
  };

  /**
   * Use the range limits to create printable labels.
   */
  public static void createLabels()
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
