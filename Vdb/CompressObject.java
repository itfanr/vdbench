package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import Utils.*;

/**
 * A class to zip and unzip a (serializable) object.
 *
 * To be used for socket traffic.
 */
public class CompressObject
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";


  public static byte[] compressObj(Object o)
  {
    try
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      GZIPOutputStream      zos = new GZIPOutputStream(bos);
      ObjectOutputStream    ous = new ObjectOutputStream(zos);

      ous.writeObject(o);
      zos.finish();
      bos.flush();

      return bos.toByteArray();
    }
    catch (Exception e)
    {
      common.failure(e);
      return null;
    }
  }


  public static Object unCompressObj(byte[] array)
  {
    try
    {
      Object obj = null;

      ByteArrayInputStream bis = new ByteArrayInputStream(array);
      GZIPInputStream      zis = new GZIPInputStream(bis);
      ObjectInputStream    ois = new ObjectInputStream(zis);

      try
      {
        obj = ois.readObject();
      }
      catch (ClassNotFoundException e)
      {
        common.failure(e);
      }

      return obj;
    }
    catch (Exception e)
    {
      common.failure(e);
      return null;
    }
  }


  public static int sizeof(Object o)
  {
    try
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream    ous = new ObjectOutputStream(bos);

      ous.writeObject(o);
      bos.flush();

      return bos.toByteArray().length;
    }
    catch (Exception e)
    {
      common.failure(e);
      return 0;
    }
  }


  public static void main(String args[]) throws Exception
  {
    test4();
  }


  public static void test4() throws IOException
  {
    common.where();
    long[] temp = new long[100];
    for (int i = 0; i < temp.length; i++)
      temp[i] = i;
    //temp += "" + rand.nextInt();

    common.ptod("sizeof: " + sizeof(temp));


    common.ptod("temp: " + temp.length * 8);
    common.ptod("temp: " + temp);
    byte[] ret = compressObj(temp);
    common.ptod("ret: " + ret.length);

    Object obj = unCompressObj(ret);
    common.ptod("obj: " + obj);

    //temp = (long[]) obj;
    //for (int i = 0; i < temp.length; i++)
    //   common.ptod("temp: " + temp[i]);
  }

}


