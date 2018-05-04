package Utils;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;


/**
 * Anchor point for all Lookup processing around NamedData().
 */
public class LookupAnchor  implements java.io.Serializable
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private String  anchor_label = null;
  private String  anchor_type  = null;
  private String  anchor_name  = null;
  private boolean use_doubles  = false;

  /* It is recommended to replace this with a hashmap or binary search! */
  private int       current_list_size  = 0;
  private  Lookup[] lookup_table  = new Lookup[100];  /* 100 should be enough! */

  /* List of class names concatenated by data label */
  private static HashMap anchor_name_list = new HashMap(16);



  public LookupAnchor(String class_name)
  {
    this(class_name, null, null);
  }


  public LookupAnchor(String class_name, String label, String type)
  {
    anchor_type  = type;
    anchor_label = label;
    anchor_name  = class_name + "/" + label;

    if (anchor_name_list.get(anchor_name) != null)
      common.failure("New LookupAnchor(): duplicate definition of " + anchor_name);
    anchor_name_list.put(anchor_name, this);
  }


  public static LookupAnchor findAnchor(String anc_name, String label)
  {
    LookupAnchor anchor = (LookupAnchor) anchor_name_list.get(anc_name + "/" + label);
    return anchor;
  }

  public String getLabel()
  {
    return anchor_label;
  }
  public String getType()
  {
    return anchor_type;
  }
  public String getAnchorName()
  {
    return anchor_name;
  }


  /**
   * The NamedData mechanism only uses long values. If a double value shows up we
   * then can not store it. We would lose too many fractions if we would.
   * Therefore, we multiply the double values by 1000 and then later on divide it
   * again by the same value.
   */
  public void setDoubles()
  {
    use_doubles = true;
  }
  public boolean useDoubles()
  {
    return use_doubles;
  }

  /**
   * Find the relative index for a specific label
   */
  public int getIndexForField(String lbl)
  {
    return getLookupForField(lbl).getIndex();
  }


  /**
   * Find the Lookup instance matching the requested label name.
   */
  public Lookup getLookupForField(String lbl)
  {
    for (int i = 0; lookup_table[i] != null; i++)
    {
      if (lookup_table[i].getFieldName().equals(lbl) ||
          lookup_table[i].getTitle().equals(lbl))
      {
        return lookup_table[i];
      }
    }

    //common.failure("Lookup.getLookupForField(): unable to find label " + lbl);
    return null;
  }

  public Lookup getLookupForIndex(int ix)
  {
    return lookup_table[ix];
  }


  /**
   * Get field titles in the order specified during field definition.
   */
  public String[] getFieldTitles()
  {
    String[] list = new String[current_list_size];

    for (int i = 0; lookup_table[i] != null; i++)
      list[i] = lookup_table[i].getTitle();

    return list;
  }
  public String[] getSortedFieldTitles()
  {
    String[] list = new String[current_list_size];

    for (int i = 0; lookup_table[i] != null; i++)
      list[i] = Format.f("%08d ", lookup_table[i].getOrder()) + lookup_table[i].getTitle();

    Arrays.sort(list);


    for (int i = 0; i < list.length; i++)
    {
      String[] split = list[i].split(" ");
      list[i] = split[1];
    }

    return list;
  }


  /**
   * Get field names in the order specified during field definition.
   */
  public String[] getFieldNames()
  {
    String[] list = new String[current_list_size];

    for (int i = 0; lookup_table[i] != null; i++)
    {
      list[i] = lookup_table[i].getFieldName();
    }

    return list;
  }


  public int getFieldCount()
  {
    return current_list_size;
  }


  public int storeLookupEntry(Lookup look)
  {
    //common.ptod("look: " + look.getFieldName());
    for (int i = 0; i < current_list_size; i++)
    {
      Lookup old = lookup_table[i];
      if (old.getFieldName().equalsIgnoreCase(look.getFieldName()) ||
          old.getTitle().equalsIgnoreCase(look.getTitle()))
        common.failure("Duplicate Lookup entry: " + look.getFieldName() + " " + look.getTitle());
    }

    lookup_table[current_list_size] = look;
    return current_list_size++;
  }


  /**
   * Validate the lookup table.
   * A list of fields is compared with the lookup table, and any new field name
   * that is found is added as a new Lookup() entry.
   *
   * A field that is NOT found in the new list is ignored. In other words:
   * we will not delete a field.
   */
  public void validateLookupTable(String[] new_list)
  {
    /* If the length does not change we're already done: */
    if (new_list.length == getFieldCount())
      return;

    String[] old_list = getFieldTitles();

    for (int i = 0; i < new_list.length; i++)
    {
      if (getLookupForField(new_list[i]) == null)
      {
        new Lookup(this, new_list[i]);
        //common.ptod("Added new Lookup entry: " + new_list[i]);
      }
    }
  }


  /**
   * Parse data received from Jni.
   * Data returned starts with all labels and ends with all counters, with
   * an asterix in between.
   */
  public long[] parseData(String data)
  {
    if (data.indexOf("*") == -1)
      common.failure("Invalid data syntax: " + data);

    /* Get all the label names: */
    String label_string = data.substring(0, data.indexOf("*"));
    StringTokenizer stl = new StringTokenizer(label_string);

    /* Get all the counters: */
    String counter_string = data.substring(data.indexOf("*") + 1);
    StringTokenizer stc = new StringTokenizer(counter_string);

    if (stc.countTokens() != stl.countTokens())
    {
      common.ptod(label_string);
      common.ptod(counter_string);
      common.failure("Unequal token count. Receiving more labels than expected");
    }

    /* Store the counters in the proper place decided by the label: */
    long[] array = new long[stc.countTokens()];
    while (stl.hasMoreTokens())
    {
      String label  = stl.nextToken();
      String number = stc.nextToken();

      /* If we can't find this field, add it: */
      if (getLookupForField(label) == null)
      {
        common.failure("Lookup missing for new field: " + label);
        //new Lookup(this, label);
        continue;
      }


      int index = getIndexForField(label);
      array[ index ] = Long.parseLong(number);
      //if (index == 0)
      //  common.ptod( index + " counters[ index ]: " + array[ index ]);
    }

    return array;
  }


  /**
   * Return an array of specific requested Lookups
   */
  public Lookup[] getSelectedLookups(String[] sels)
  {
    Lookup[] list = new Lookup[sels.length];
    for (int i = 0; i < sels.length; i++)
      list[i] = getLookupForField(sels[i]);

    return list;
  }


  public static void main(String[] args)
  {
    //NfsV3 nfs3 = new NfsV3();
    //String[] list = new String[] {"read", "better", "better2" };

    //nfs3.validateLookupTable(list);

    //String[] fields = nfs3.getFieldTitles();
    //for (int i = 0; i < fields.length; i++)
    //  common.ptod("title: " + fields[i]);
    //common.ptod("
    //            /: " + /  common.ptod("title: " + fields[i]);


  }
}
