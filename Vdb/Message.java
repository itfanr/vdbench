package Vdb;
    
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

  /**
   * Centers the frame on the screen
   */
  public static void centerscreen(Container container)
  {
    Dimension screen = container.getToolkit().getScreenSize();
    Rectangle bounds = container.getBounds();
    container.setLocation((screen.width  - bounds.width) / 2,
                          (screen.height - bounds.height) / 2);
    container.requestFocus();
  }

  public static void setDimensions(JComponent comp, Dimension dim)
  {
    comp.setMinimumSize(dim);
    comp.setMaximumSize(dim);
    comp.setPreferredSize(dim);
  }
  public static void setDimensions(JComponent comp, int width, int height)
  {
    Dimension dim = new Dimension(width, height);
    setDimensions(comp, dim);
  }

}

