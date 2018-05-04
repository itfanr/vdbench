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

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

/* Maybe create something like %8m %8k, where we report in m or k if the value
   does not fit the length?
   */

/**
 * This class allows for a primitive 'printf()' style printing.
 * You may code %d, %f, and %s
 */
public class Format
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  /*                                           15              6          */
  static DecimalFormat df = new DecimalFormat("##############0.000000");


  /**
   * Pick up input data and pass it along.
   */
  public static String f(String form, int value)
  {

    String ret = doit(form, null, (double) value);
    return ret;
  }

  public static String f(String form, long value)
  {
    String ret = doit(form, null, (double) value);
    return ret;
  }

  public static String f(String form, double value)
  {
    if (value == Double.MAX_VALUE)
      value = 0;

    String ret = doit(form, null, (double) value);
    return ret;
  }

  public static String f(String form, String value)
  {

    if (value.length() != 0)
      return doit(form, value, 0);
    else
      return doit(form, " ", 0);
  }

  /**
   * Long hex printing has a bug because the temporary long-to-double
   * translation loses precission.
   */
  public static String X(long hex)
  {
    long tmp   = hex >>> 32;
    String txt = Format.f("%08x", tmp);
    tmp        = hex << 32 >>> 32;
    txt       += Format.f("%08x ", tmp);
    return txt;
  }

  /**
   * Process format request.
   * Interpret the format, and then split the input into 'digits' and
   * 'decimals' by splitting it after the first '.'
   * The input value has been translated already from a numeric value
   * to a printable value using standard Java.
   */
  public static String doit(String form, String value, double number)
  {
    String chars = form;
    StringBuffer out = new StringBuffer("");
    int idx = 0;
    int length, lengthd;
    char type = 0;
    String pounds = "##################################################" +
                    "##################################################" +
                    "##################################################";
    String zeroes = "00000000000000000000000000000000000000000000000000" +
                    "00000000000000000000000000000000000000000000000000" +
                    "00000000000000000000000000000000000000000000000000";
    String blanks = "                                                  " +
                    "                                                  " +
                    "                                                  ";
    String front  = blanks;
    String back   = zeroes;
    boolean left = false;
    boolean longhex = false;


    try
    {

      for (int i = 0; i < chars.length(); i++)
      {
        /* Wait for single '%': */
        if (chars.charAt(i) == '%')
        {
          if (i+1 >= chars.length())          // no more chars after '%'
            break;
          if (chars.charAt(i+1) == '%')       // double '%%'
          {
            out.append('%');                  // return one '%'
            i++;
            continue;
          }

          /* We have '%': */
          i++;
          length = lengthd = 0;

          /* Check for zerofill and left adjust: */
          if (chars.charAt(i) == '0')
          {
            i++;
            front = zeroes;
          }
          else if (chars.charAt(i) == '-')
          {
            i++;
            left = true;
          }

          /* Count frontend length (digits): */
          while (chars.charAt(i) >= '0' && chars.charAt(i) <= '9')
          {
            length *= 10;
            length += chars.charAt(i++) - '0';
          }

          /* Count backend length (decimals): */
          if (chars.charAt(i) == '.')
          {
            i++;
            while (chars.charAt(i) >= '0' && chars.charAt(i) <= '9')
            {
              lengthd *= 10;
              lengthd += chars.charAt(i++) - '0';
            }
          }

          /* With decimals the length includes the '.' and the decimals: */
          if (lengthd != 0 && length != 0)
            length = (length - lengthd - 1 > 0 ) ? length - lengthd - 1 : 1;

          /* Get format type: */
          type = chars.charAt(i);
          if (type == 'X')
            longhex = true;
        else if (type != 'f' && type != 'F' && type != 'd' && type != 'x' && type != 's')
            common.failure("Invalid format string: " + form + "/" + type);

          if (left && type != 's')
            common.failure("left adjust only allowed for strings: '" + form + "'");

        if (type == 'f' || type == 'F' || type == 'd')
          {

            //common.ptod("value: " + value + " form: " + form + " length: " + length + " lengthd: " + lengthd + " number: " + number);
            DecimalFormat ff = new DecimalFormat("#0.0");
            ff.setMinimumIntegerDigits(length);
            ff.setMinimumFractionDigits(lengthd);
            ff.setMaximumFractionDigits(lengthd);

            // After a call from Pablo.Alonso@Sun.COM
            // I should start using the new String.format() but that's a lot of work
            DecimalFormatSymbols symbols = ff.getDecimalFormatSymbols();
            symbols.setDecimalSeparator('.');
            ff.setDecimalFormatSymbols(symbols);

            /* Do a little rounding for very small floating numbers */
            /* as in  7.934999999999999 --> 7.935 */
            number += .000000000000001;

            String formatted = ff.format(number);
            //common.ptod("formatted: " + formatted);

            /* Remove leading zeros: */
            int j = 0;
            for (j = 0; j < formatted.length() - 1; j++)
            {
              if (formatted.charAt(j) != '0' || formatted.charAt(j+1) == '.')
                break;
            }
            if (j != formatted.length())
              formatted = front.substring(0,j) + formatted.substring(j);

          //common.ptod("length: " + length);
          //common.ptod("lengthd: " + lengthd);
          //common.ptod("formatted: >" + formatted + "<");
          //common.ptod("number: " + number);

          //common.ptod("type: " + type);
          if (type == 'F' && formatted.length() > length + lengthd + 1)
            formatted = fitIt(formatted, length, lengthd);

            out.append(formatted);

            continue;
          }

          /* If we have digits, collect them: */
          String digits = value;

          // full double 'X' loses precission!!!

          if (type == 'x' || type == 'X')
          {
            digits = Long.toHexString((long) number);

            if (!longhex)
            {
              int len = digits.length();
              if (len > 8)
                digits = digits.substring(len - 8);
            }
          }

          /* Add fill bytes and left adjustment: */
          int  extras = length - digits.length();
          if (extras > 0)
          {
            if (left)
              digits = digits + front.substring(0, extras);
            else
              digits = front.substring(0, extras) + digits;
          }

          /* Copy (left) digits to output: */
          out.append(digits);

          //common.ptod("digits: " + digits + "    " + number);

          /* Get decimal digits if requested: */
          String decimals = value;

          if (lengthd != 0)
          {
            if (value.indexOf(".") != -1)
              decimals = value.substring(value.indexOf(".") + 1);

            if (lengthd > decimals.length())
              decimals =  decimals + back.substring(0, lengthd - decimals.length());
            else if (lengthd < decimals.length())
              decimals = decimals.substring(0, lengthd);


            out.append("." + decimals);
          }
        }

        else
        {
          out.append(chars.charAt(i));
        }
      }
    }

    catch (Exception e)
    {
      common.ptod("Exception in Format.doit(): ");
      //e.printStackTrace();
      common.ptod("form: " + form + " value: " + value);
      common.failure(e);
    }


    return  String.valueOf(out);
  }


  static String zeroes = "0000000000000000";



  /**
   * Translate a long/int to a hex string of requested length with leading zeroes.
   * If the resulting hex string (without leading zeroes) is longer than the
   * requested length, the longer length is returned.
   */
  public static String hex(long number)
  {
    return hex(number, 1);
  }
  public static String hex(long number, int len)
  {
    String hexnum, result;

    if (len == 0 || len > 16)
      common.failure("hex(): Length must be between 1 and 16 (" + len + ")");

    hexnum = Long.toHexString(number);
    if (hexnum.length() > len)
      return hexnum;

    result = zeroes.substring(0, 16 - hexnum.length()) + hexnum;
    result = result.substring(16 - len);
    return result;
  }

  public static String hex(int number)
  {
    return hex(number, 1);
  }
  public static String hex(int number, int len)
  {
    String hexnum, result;

    if (len == 0 || len > 8)
      common.failure("hex(): Length must be between one and 8 (" + len + ")");

    hexnum = Integer.toHexString(number);
    if (hexnum.length() > len)
      return hexnum;

    result = zeroes.substring(0, 8 - hexnum.length()) + hexnum;
    result = result.substring(8 - len);
    return result;
  }


  /**
   * There is too much data to fit inside of the amount of bytes that we
   * may use. Remove decimals.
   */
  private static String fitIt(String txt, int length, int lengthd)
  {
    int size = length + lengthd + 1;
    String blanks = "                                                  ";

    /* If there are no decimals, just return: */
    if (lengthd == 0)
      return txt;

    /* Remove the last decimals: */
    String tmp = txt;
    while (true)
    {
      tmp = tmp.substring(0, tmp.length() - 1);
      //common.ptod("tmp: " + tmp);

      /* Make sure we don't end with a period: */
      if (tmp.endsWith("."))
        continue;

      //common.ptod("tmp: " + tmp);
      //common.ptod("tmp.length(): " + tmp.length());
      //common.ptod("size: " + size);

      /* If we already lost the '.', then it HAS to fit because we can't make it smaller: */
      if (tmp.indexOf(".") == -1)
      {
        if (tmp.length() >= size)
          return tmp;
        String tmp2 = blanks.substring(0, size - tmp.length()) + tmp;
        return tmp2;
      }

      /* If it fits, exit with the proper length: */
      if (tmp.length() <= size)
      {
        if (tmp.length() == size)
          return tmp;
        else
        {
          String tmp2 = blanks.substring(0, size - tmp.length()) + tmp;
          //common.ptod("tmp2: " + tmp2);
          return tmp2;
        }
      }
    }

  }




  public static void main(String args[])
  {
  }

}
