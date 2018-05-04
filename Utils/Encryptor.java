package Utils;

/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import  java.io.*;
import  java.util.*;


/**
 * <p>Title: Encryptor.java</p>
 * <p>Description: A class to encrypt text data using the shift cipher.
 * This is not intended to be difficult to decrypt, but
 * rather to serve to slightly obfuscate various kinds of data for situations
 * which do not require much security.</p>
 * @author Jeff Shafer
 * @version 1.0
 */

public class Encryptor
{
  private final static String c =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";

  // This is the size of our "alphabet".  It is the number
  // of characters in ASCII.
  private final static int ASCII_RANGE = 256;

  // The number of characters by which we wish to shift.
  private static byte shiftRange = 50;

  /**
   * Allows the user to set the key to something other than the default value
   * if he desires.
   * @param key a byte value containing the encryption key.
   */
  public static void setKey(byte key)
  {
    shiftRange = key;
  }

  /**
   * Returns the input String as an encrypted string, using the shift
   * cipher.
   * @param data a plaintext String.
   * @return an encrypted String.
   */
  public static String encrypt(String data)
  {
    char[] inputData = data.toCharArray();
    char[] outputData = new char[inputData.length];
    for(int i = 0; i < inputData.length; i++)
    {
      outputData[i] = (char)(((int)inputData[i] + shiftRange) % ASCII_RANGE);
    }
    return String.valueOf(outputData);
  }


  /**
   * Returns the input String as a decrypted string, using the shift
   * cipher.
   * @param data an encrypted String.
   * @return a decrypted plaintext String.
   */
  public static String decrypt(String data)
  {
    char[] inputData = data.toCharArray();
    char[] outputData = new char[inputData.length];
    for(int i = 0; i < inputData.length; i++)
    {
      outputData[i] = (char)(((int)inputData[i] - shiftRange) % ASCII_RANGE);
    }
    return String.valueOf(outputData);
  }

}
