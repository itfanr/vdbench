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
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import Utils.Format;
import Utils.common;

/**
 * Allow customer to modify the percentage ranges and colors
 */
public class ChangeRanges extends JDialog implements ActionListener, MouseListener
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private JButton ok_button = new JButton("OK");
  private JButton cancel    = new JButton("Cancel");
  private JButton dflt      = new JButton("Default");

  private int     current_delta;
  private Delta[] deltas          = Delta.getDeltas();
  private JTextField[] txt_fields = new JTextField[deltas.length];
  private JTextField[] color_fields = new JTextField[deltas.length];
  private WlComp wlcomp           = null;

  public ChangeRanges(WlComp wl)
  {
    setModal(true);
    setTitle("Vdbench Compare: change colors or ranges");
    wlcomp = wl;
    getContentPane().setLayout(new GridLayout(deltas.length + 3, 2));


    for (int i = 0; i < deltas.length; i++)
    {
      Delta delta = deltas[i];
      txt_fields[i] = new JTextField();
      txt_fields[i].setHorizontalAlignment(JTextField.RIGHT);
      txt_fields[i].setText("" + delta.limit);

      /* The Delta instance may not be active in two places at the time. */
      /* I therefore must duplicate the JTextField:                      */
      JTextField tf = new JTextField();
      color_fields[i] = tf;
      tf.setBackground(delta.color);
      tf.setHorizontalAlignment(JTextField.CENTER);
      tf.setEditable(true);

      tf.addMouseListener(this);

      getContentPane().add(tf);
      getContentPane().add(txt_fields[i]);

      if (delta.limit == 0)
        txt_fields[i].setEnabled(false);
    }

    getContentPane().add(ok_button);
    //getContentPane().add(cancel);
    getContentPane().add(dflt);
    getContentPane().add(new JLabel("Click on color or range to change"));

    ok_button.addActionListener(this);
    cancel.addActionListener(this);
    dflt.addActionListener(this);

  }

  public static void main(String[] args)
  {

    ChangeRanges cr = new ChangeRanges(null);

    cr.addWindowListener(new WindowAdapter()
                         {
                           public void windowClosing(WindowEvent e)
                           {
                             System.exit(0);
                           }
                         });

    cr.setLocation(120, 120);
    cr.setSize(300,300);

    //cr.pack();
    cr.setVisible(true);

  }


  public void actionPerformed(ActionEvent e)
  {
    String cmd = e.getActionCommand();

    /* OK button: check for limits first before accepting them: */
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

      if (wlcomp.table_panel != null)
        wlcomp.doCompare();

      this.dispose();
    }

    /* Cancel, just exit: */
    else if (cmd.equals(cancel.getText()))
      this.dispose();

    /* Default: go back to the hardcoded defaults: */
    else if (cmd.equals(dflt.getText()))
    {
      Delta.setDefaults();
      for (int i = 0; i < color_fields.length; i++)
      {
        color_fields[i].setBackground(deltas[i].color);
        txt_fields[i].setText("" + deltas[i].limit);
        deltas[i].setBackground(deltas[i].color);
      }
      Delta.createLabels();
      this.repaint();
      if (wlcomp.table_panel != null)
        wlcomp.table_panel.repaint();
      //setVisible(true);
    }
  }



  public void mouseExited(java.awt.event.MouseEvent me)
  {
  }
  public void mouseEntered(java.awt.event.MouseEvent me)
  {
  }
  public void mouseReleased(java.awt.event.MouseEvent me)
  {
  }
  public void mousePressed(java.awt.event.MouseEvent me)
  {
  }

  /**
   * A click was done on a color field.
   */
  public void mouseClicked(java.awt.event.MouseEvent me)
  {
    Color old_color = getBackground();
    Color new_color = JColorChooser.showDialog(null, "xxx", old_color);
    if (new_color == null)
      return;

    /* Which field? */
    for (int i = 0; i < color_fields.length; i++)
    {
      if (color_fields[i] == me.getComponent())
      {
        color_fields[i].setBackground(new_color);
        deltas[i].setBackground(new_color);
        deltas[i].color = new_color;
        color_fields[i].repaint();
      }
    }

    repaint();
  }
}
