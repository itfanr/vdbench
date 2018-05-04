package Utils;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.awt.event.*;
import java.awt.*;
import java.io.*;
import javax.swing.*;
import javax.swing.border.*;



/**
 * Create a simple full screen editor
 */
public class Editor extends JFrame implements ActionListener

{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  public Editor(String title)
  {
    this.setTitle(title);
    this.addWindowListener(new WindowAdapter()
                           {
                             public void windowClosing(WindowEvent e)
                             {
                               System.exit(0);
                             }
                           }
                          );
  }

  public Editor()
  {
    super("Editor");
  }


  private static JEditorPane edit_pane = null;
  private static String edit_file = null;


  /* Create buttons and a filler: */
  static JButton save = new JButton("Save");
  static JButton done = new JButton("Cancel");
  static JButton both = new JButton("Save and exit");
  static JLabel  fill = new JLabel();


  public static void main(String args[]) throws Exception
  {
    /* Give a little helping hand: */
    if (args.length == 0)
    {
      System.err.println("Usage: vdbench edit file.name");
      System.exit(-1);
    }

    /* It appears JEditorPane wants the complete file name: */
    edit_file = new File(args[0]).getAbsolutePath();

    /* If the file does not exist, create an empty one: */
    if (!new File(edit_file).exists())
    {
      Fput fp = new Fput(edit_file);
      fp.println("*Empty file, created by the Vdbench Ultra Cheap Fullscreen Editor");
      fp.close();
    }

    JScrollPane scroll_rsh = new JScrollPane();
    scroll_rsh.setBorder(new TitledBorder(new EtchedBorder(EtchedBorder.RAISED,Color.white,new Color(178, 178, 178)),
                                          "Editing file " + edit_file));

    /* Allocate the Frame and set it up: */
    Editor tf = new Editor("Ultra Cheap Fullscreen Editor");
    tf.setSize(new Dimension(1000,800));
    Container cp = tf.getContentPane();
    cp.setLayout(new GridBagLayout());

    /* This JEditorPane hold ans displays the filea: */
    edit_pane = new JEditorPane("file:///" + edit_file);

    /* Make sure you use fixed font for editing: */
    edit_pane.setFont(new Font("Courier New", Font.PLAIN, 12));

    save.addActionListener(tf);
    done.addActionListener(tf);
    both.addActionListener(tf);

    /* Allow scrolling, and and everything to the frame: */
    scroll_rsh.getViewport().add(edit_pane, null);
    cp.add(scroll_rsh, new GridBagConstraints(0, 0, 4, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,       new Insets(0, 0, 0, 0), 0, 0));
    cp.add(save,       new GridBagConstraints(0, 1, 1, 1, 0.1, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,       new Insets(0, 0, 0, 0), 0, 0));
    cp.add(done,       new GridBagConstraints(1, 1, 1, 1, 0.1, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,       new Insets(0, 0, 0, 0), 0, 0));
    cp.add(both,       new GridBagConstraints(2, 1, 1, 1, 0.1, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,       new Insets(0, 0, 0, 0), 0, 0));
    cp.add(fill,       new GridBagConstraints(3, 1, 1, 1, 0.9, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH,       new Insets(0, 0, 0, 0), 0, 0));

    Message.centerscreen(tf);
    tf.setVisible(true);
  }

  public void actionPerformed(ActionEvent e)
  {
    String cmd = e.getActionCommand();

    if (e.getActionCommand().equals("Cancel"))
      System.exit(0);

    else if (e.getActionCommand().equals("Save"))
    {
      String data = edit_pane.getText().trim();
      Fput fp = new Fput(edit_file);
      fp.println(data);
      fp.close();
    }

    else if (e.getSource() == both)
    {
      String data = edit_pane.getText().trim();
      Fput fp = new Fput(edit_file);
      fp.println(data);
      fp.close();
      System.exit(0);
    }

    else if (!e.getActionCommand().equals("Cancel"))
    {
      System.err.println("bad action: " + cmd);
      System.exit(0);
    }
  }
}
