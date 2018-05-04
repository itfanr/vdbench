package Utils;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

/**
 * This class gets called each time a new line of output is found in the
 * output of OS_cmd()
 */
public abstract class CommandOutput
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  /**
   * Call requestor with a new line of data.
   *
   * -line: new data
   * -type: either stderr or stdout
   *
   * A return of 'false' means to kill the OS_cmd() command currently
   * running.
   *
   */
  abstract public boolean newLine(String line, String type, boolean more);
}


