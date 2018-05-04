package VdbComp;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.lang.RuntimeException;



/**
 * Exception that will allow miost errors to just display and error message
 * and allow the user to continue.
 */
public class CompException extends RuntimeException
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  public CompException(String txt)
  {
    super(txt);
  }
}
