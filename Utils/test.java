package Utils;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import Utils.Format;

public class test
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  /** Creates a new instance of Main */

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args)
  {

    // TODO code application logic here
    long OVERFLOW_ADJUSTMENT = (long)Math.pow(2, 64);
    System.out.println("OVERFLOW ADJUSTMENT =" + Long.toHexString (OVERFLOW_ADJUSTMENT));
    System.out.println("MAXLONG = " + Long.toHexString (Long.MAX_VALUE));
    System.out.println("MINLONG = " + Long.toHexString (Long.MIN_VALUE));

    long value = Long.MAX_VALUE + 2;
    System.out.println("Long.MAX_VALUE=              " + Long.MAX_VALUE);
    System.out.println("value= Long.MAX_VALUE + 2 = " + value);

    long value1 = value;

    if (value < 0)
      value1 -= Long.MAX_VALUE;

    System.out.println("value1: " + value + " - Long.MAX_VALUE =  " + value1);

    long  value2 = value & OVERFLOW_ADJUSTMENT;
    System.out.println("value2: " + value + " &  OVERFLOW_ADJUSTMENT = " + value2);



    for (int i = 0; i < 10; i++)
    {
      long value3 = Long.MAX_VALUE;
      value3 += i;

      value3 = (value3 << 1 >>> 1);

      String line = "value3 + " + i + ": " + value3 ;

      System.out.println(line);
    }

  }

}
