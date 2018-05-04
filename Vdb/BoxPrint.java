package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.Vector;




/**
 * This class gathers some text dasta and then prints it inside of a box.
 */
public class BoxPrint
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private ArrayList <String> lines = new ArrayList(8);

  public BoxPrint()
  {
  }


  public void add(String format, Object ... args)
  {
    if (args.length > 0)
      lines.add(String.format(format, args));
    else
      lines.add(format);
  }


  public int size()
  {
    return lines.size();
  }
  public void clear()
  {
    lines.clear();
  }

  public void sort()
  {
    Collections.sort(lines);
  }

  /**
   * Print only to the console and logfile.
   * Note: print will be silently ignored when no data has been added.
   */

  public void print()
  {
    if (lines.size() == 0)
      return;

    common.ptod(getLines());
  }
  public Vector <String> getLines()
  {
    Vector <String> txt = new Vector(lines.size() + 10);

    int max_length = 0;
    for (String line : lines)
      max_length = Math.max(max_length, line.length());

    String mask        = "* %-" + max_length + "s *";
    StringBuffer stars = new StringBuffer(256);
    for (int i = 0; i < max_length + 4; i++)
      stars.append("*");


    txt.add("*");
    txt.add(stars.toString());

    for (String line : lines)
      txt.add(String.format(mask, line));

    txt.add(stars.toString());
    txt.add("*");

    return txt;
  }

  public static void printOne(String format, Object ... args)
  {
    BoxPrint box = new BoxPrint();
    box.add(format,args);
    box.print();
  }

  public static Vector <String> getOne(String format, Object ... args)
  {
    BoxPrint box = new BoxPrint();
    box.add(format,args);
    return box.getLines();
  }

  /**
   * Print to the console, summary, and logfile.
   */
  public void printSumm()
  {
    if (lines.size() == 0)
      return;

    int max_length = 0;
    for (String line : lines)
      max_length = Math.max(max_length, line.length());

    String mask        = "* %-" + max_length + "s *";
    StringBuffer stars = new StringBuffer(256);
    for (int i = 0; i < max_length + 4; i++)
      stars.append("*");


    common.pboth("*");
    common.pboth(stars.toString());

    for (String line : lines)
      common.pboth(mask, line);

    common.pboth(stars.toString());
    common.pboth("*");
  }

  public static void main(String[] args)
  {
    BoxPrint box = new BoxPrint();
    box.add("Hello");
    box.add("World");
    box.print();
    box.print();

    BoxPrint.printOne("hellO");
  }
}


