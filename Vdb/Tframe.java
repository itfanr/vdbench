package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.awt.event.*;
import java.awt.*;
import java.io.*;
import javax.swing.*;
import javax.swing.border.*;
import Utils.Fput;



/**
 * Create a simple frame for debugging
 */
public class Tframe extends JFrame

{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  public Tframe(String title)
  {
    this.setTitle(title);
    this.addWindowListener(new WindowAdapter()
                           {
                             public void windowClosing(WindowEvent e)
                             {
                               common.exit(0);
                             }
                           }
                          );
    this.setSize(600, 400);
  }

  public Tframe()
  {
    this.setTitle("Tframe");
    this.addWindowListener(new WindowAdapter()
                           {
                             public void windowClosing(WindowEvent e)
                             {
                               common.exit(0);
                             }
                           }
                          );
    this.setSize(600, 400);
  }


  private static JEditorPane je = null;
  private static String edit_file = null;

  public static void main(String args[]) throws Exception
  {

    Tframe frame = new Tframe();
    frame.setSize(300,300);
    Message.centerscreen(frame);
    frame.setVisible(true);
  }

}
