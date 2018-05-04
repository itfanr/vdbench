package Utils;

/*
 * Copyright 2010 Sun Microsystems, Inc. All rights reserved.
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * The contents of this file are subject to the terms of the Common
 * Development and Distribution License("CDDL") (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at http://www.sun.com/cddl/cddl.html
 * or ../vdbench/license.txt. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice
 * in each file and include the License file at ../vdbench/licensev1.0.txt.
 *
 * If applicable, add the following below the License Header, with the
 * fields enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
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
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

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


  // A short main to test.
  public static void main(String[] args) throws Exception
  {
    String owner =

    "userName:           Henk Vandenbergh \n"     +
    "userTitle:          Performance Engineer \n" +
    "userCompanyName:    Sun Microsystems \n"     +
    "userCompanyAddress: Broomfield"                 ;

    String encrypted = Encryptor.encrypt(owner);
    String decrypted = Encryptor.decrypt(encrypted);

    System.out.println("Original text: " + owner);
    System.out.println("Encrypted text: " + encrypted);
    System.out.println("Decrypted text: " + decrypted);

    common.serial_out("owner.txt", encrypted);

    decrypted = Encryptor.decrypt((String) common.serial_in("owner.txt"));


    //System.out.println("Decrypted text: " + decrypted);
  }
}
