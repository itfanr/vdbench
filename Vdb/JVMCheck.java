package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.*;


public class JVMCheck
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  // Determines the users current JRE version, and splits the resulting string into
  // numerical tokens.  These tokens are then evaluated to be sure that the JRE
  // version is 1.4.2 or higher.
  public static boolean isJREValid(String javaVersionString,
                                   int    version,
                                   int    release,
                                   int    modification)
  {
    boolean debug = false;
    // If the version has not been set, return false.
    if (javaVersionString == null || javaVersionString == "")
    {
      return false;
    }

    // Get the version as a string of integers (and possibly embedded
    // underscores) of the form "a.b.c".
    // Split the version string into elements, according to "." and """.
    StringTokenizer tokenizer = new StringTokenizer(javaVersionString, "\".");
    int[] versionElements = new int[3];

    // Parse the string as a series of ints, making sure that we keep only the first element
    // of each substring provided by the tokenizer.  This must be done because of version
    // strings of the form 1.4.1_02!  We only need the first digit of each substring!
    // Note that parsing can throw a number format exception if the string being parsed
    // is not of the correct form, in which case we must return false in the following catch
    // statement.
    try
    {
      for (int i = 0; i < 3 && tokenizer.hasMoreTokens(); i++)
      {
        versionElements[i] = Integer.parseInt(tokenizer.nextToken().substring(0, 1));
        //System.out.println("versionElements[" + i + "]: " + versionElements[i]);
      }
    }
    catch (NumberFormatException nfe)
    {
      return false;
    }

    if (debug)
    {
      common.ptod("javaVersionString: " + javaVersionString);
      common.ptod("versionElements[0]: " + versionElements[0]);
      common.ptod("versionElements[1]: " + versionElements[1]);
      common.ptod("versionElements[2]: " + versionElements[2]);
      common.ptod("version: " + version);
      common.ptod("release: " + release);
      common.ptod("modification: " + modification);
    }

    // If the version is lower, kill:
    if (versionElements[0] < version)
    {
      if (debug) common.where();
      return false;
    }

    // If the version is greater, accept:
    if (versionElements[0] > version)
    {
      if (debug) common.where();
      return true;
    }

    // If the release is lower, kill:
    if (versionElements[1] < release)
    {
      if (debug) common.where();
      return false;
    }

    // If the release is greater, accept:
    if (versionElements[1] > release)
    {
      if (debug) common.where();
      return true;
    }

    // If the modification is lower, kill:
    if (versionElements[2] < modification)
    {
      if (debug) common.where();
      return false;
    }

    // If the modification is greater, accept:
    if (versionElements[2] > modification)
    {
      if (debug) common.where();
      return true;
    }

    if (debug) common.where();
    // It all is an EXACT math, accept:
    return true;
  }


  /**
   * This is for debugging.
   */
  private static void test(String input,
                           int version,
                           int release,
                           int modification,
                           boolean result)
  {
    boolean rc = isJREValid(input, version, release, modification);

    System.out.println("JVMCheck input: " + input + "; " +
                       version + "." + release + "." + modification +
                       " " + rc);

    if (rc != result)
      System.out.println("========== This result should have been " + result);
  }



  public static void main(String[] args)
  {
    System.out.println(System.getProperty("java.version"));

    test("1.3.0",    1,4,2, false);
    test("1.4.0",    1,4,2, false);
    test("1.4.1",    1,4,2, false);
    test("1.4.1_02", 1,4,2, false);
    test("1.4.2",    1,4,2, true);
    test("1.4.3",    1,4,2, true);
    test("1.5.0",    1,4,2, true);
    test("2.5.0",    1,4,2, true);

  }

}

