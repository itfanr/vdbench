package Utils;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
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
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

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

