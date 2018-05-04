package Utils;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.*;

/**
 * NFS V4 Kstat statistics.
 *
 * These field names below only need to be specified if we want to use
 * any direct Lookup. In other words, see these as #defines
 */
public class NfsV4 extends NamedData implements java.io.Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  private static LookupAnchor anchor = new LookupAnchor("Utils.NfsV4", "NfsV4", "nfsstat");

  /* Specify fields in order of preferred reporting: */
  public static Lookup CREATE              = new Lookup(anchor, "create     ");
  public static Lookup OPEN                = new Lookup(anchor, "open       ");
  public static Lookup CLOSE               = new Lookup(anchor, "close      ");
  public static Lookup READ                = new Lookup(anchor, "read       ");
  public static Lookup WRITE               = new Lookup(anchor, "write      ");
  public static Lookup GETATTR             = new Lookup(anchor, "getattr    ");
  public static Lookup SETATTR             = new Lookup(anchor, "setattr    ");
  public static Lookup ACCESS              = new Lookup(anchor, "access     ");
  public static Lookup LOOKUP              = new Lookup(anchor, "lookup     ");
  public static Lookup GETFH               = new Lookup(anchor, "getfh      ");
  public static Lookup PUTFH               = new Lookup(anchor, "putfh      ");

  public static Lookup COMPOUND            = new Lookup(anchor, "compound   ");
  public static Lookup RESERVED            = new Lookup(anchor, "reserved   ");
  public static Lookup COMMIT              = new Lookup(anchor, "commit     ");
  public static Lookup DELEGPURGE          = new Lookup(anchor, "delegpurge ");
  public static Lookup DELEGRETURN         = new Lookup(anchor, "delegret   ",  "delegreturn");
  public static Lookup LINK                = new Lookup(anchor, "link       ");
  public static Lookup LOCK                = new Lookup(anchor, "lock       ");
  public static Lookup LOCKT               = new Lookup(anchor, "lockt      ");
  public static Lookup LOCKU               = new Lookup(anchor, "locku      ");
  public static Lookup LOOKUPP             = new Lookup(anchor, "lookupp    ");
  public static Lookup NVERIFY             = new Lookup(anchor, "nverify    ");
  public static Lookup OPENATTR            = new Lookup(anchor, "openattr   ");
  public static Lookup OPEN_CONFIRM        = new Lookup(anchor, "open_conf  ",  "open_confirm");
  public static Lookup OPEN_DOWNGRADE      = new Lookup(anchor, "open_dwngr ",  "open_downgrade");
  public static Lookup PUTPUBFH            = new Lookup(anchor, "putpubfh   ");
  public static Lookup PUTROOTFH           = new Lookup(anchor, "putrootfh  ");
  public static Lookup READDIR             = new Lookup(anchor, "readdir    ");
  public static Lookup READLINK            = new Lookup(anchor, "readlink   ");
  public static Lookup REMOVE              = new Lookup(anchor, "remove     ");
  public static Lookup RENAME              = new Lookup(anchor, "rename     ");
  public static Lookup RENEW               = new Lookup(anchor, "renew      ");
  public static Lookup RESTOREFH           = new Lookup(anchor, "restorefh  ");
  public static Lookup SAVEFH              = new Lookup(anchor, "savefh     ");
  public static Lookup SECINFO             = new Lookup(anchor, "secinfo    ");
  public static Lookup SETCLIENTID         = new Lookup(anchor, "setclient  ",  "setclientid");
  public static Lookup SETCLIENTID_CONFIRM = new Lookup(anchor, "setcl_conf ",  "setclientid_confirm");
  public static Lookup VERIFY              = new Lookup(anchor, "verify     ");


  public NfsV4()
  {
    super(anchor);
  }
}



