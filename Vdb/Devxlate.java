package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.util.Vector;
import java.io.*;
import java.util.StringTokenizer;
import java.io.Serializable;
import Utils.Format;
import Utils.Fget;
import Utils.OS_cmd;
import Utils.Getopt;


/*
 Notes for powerpath:
 when vdbench addresses cxtxdxsx, pp picks up different cxtxdxsx luns
 to do its multi pathing to. So, for instance if you only read from
 cxtxd1s0 you can see kstat accumulate data for cxtxd2s0/d3s0, etc. making
 individual counters useless since they don't show the i/o to the real cxtxd1s0.

 For TNF:
 One trace showed strategy cxtx biodone cxtx biodone emcpower0a. This gives proper
 results for cxtx.
 An other trace however showed it in reverse: strategy emcpower0a, biodone emcpower0a,
 biodone cxtx.
 No real device activity showed up under 'real controllers'. The emc numbers
 went to 'undefined' controller.

 /etc/powermt allows you to xlate cxtx to emcpowerxx

*/

public class Devxlate
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  String fullname;   /* from iostat -xdn                             */
  String instance;   /* From iostat -xd                              */
  String special;              /* From mnttab                                 */
  long   kstat_pointer;
  DevicePieces dev_pieces = null;

  int    seqno;

  boolean kstat_active;        /* Device used in this run?                    */

  private static String   last_zpool_checked = "never_used_yet";
  private static String[] zfs_stdout = null;

  private static String cols[] = new String[40];
  private static int    col = 0;

  private static Vector mnttab = null;
  private static Vector vfstab = null;
  private static Vector mcf    = null;
  static Vector instance_list   = new Vector(64, 0);

  public static  Vector active_list;

  private static Vector search_list; /* Search list */
  private static int sequence_number = 0;


  static boolean debug = common.get_debug(common.DEVXLATE);

  public Devxlate(String full)
  {
    fullname = full.toLowerCase();
    seqno = sequence_number++;
  }


  /**
   * Issue OS command and return output lines in a String Vector
   *
   * (This is a wrapper around OS_cmd() because of OS_cmd() replacing the original
   * OS command methods)
   *
   */
  private static boolean os_command(String command, Vector std_out, Vector std_err)
  {
    OS_cmd ocmd = new OS_cmd();
    ocmd.addText(command);
    boolean rc = ocmd.execute(false);

    String[] out = ocmd.getStdout();
    for (int i = 0; i < out.length; i++)
      std_out.add(out[i]);

    String[] err = ocmd.getStderr();
    for (int i = 0; i < err.length; i++)
      std_err.add(err[i]);

    return rc;
  }


  /**
   * Read vfstab and see if there is a match for this full filename
   */
  public static boolean get_vfstab(String fname)
  {

    /* Read file if needed: */
    if (vfstab == null)
    {
      vfstab = Fget.read_file_to_vector("/etc/vfstab");
    }


    /* Get the mount point for each entry: */
    for (int i = 0; i < vfstab.size(); i++)
    {
      String line = (String) vfstab.elementAt(i);
      StringTokenizer st = new StringTokenizer(line);
      if (st.countTokens() < 4)
        continue;

      String device = st.nextToken();
      st.nextToken();
      String mp = st.nextToken();
      String fs = st.nextToken();
      if (!mp.startsWith("/"))
        continue;

      /* Add a '/' to this mountpoint so that we can do a 'startswith()': */
      if (fname.startsWith(mp + "/"))
      {
        /* We have a match, pick up the file system type: */
        if (fs.equalsIgnoreCase("samfs") || fs.equalsIgnoreCase("qfs"))
        {
          get_qfs_list(device);
          return true;
        }
      }
    }

    return false;
  }

  public static void clearMnttab()
  {
    mnttab = null;
  }

  /**
   * Read and interpret /etc/mnttab data.
   *
   * Be aware that automount file system (autofs) will not be included until
   * they are actually mounted.
   */
  public static String[] get_mnttab(String fname, boolean use_mount_point)
  {
    /* Read mnttab only when needed: */
    if (mnttab == null)
      readAndSortMnttab();

    if (debug) common.ptod("get_mnttab fname: %s use_mount_point: %b", fname, use_mount_point);

    for (int i = 0; i < mnttab.size(); i++)
    {
      //special   mount_point   fstype   options   time
      String line = (String) mnttab.elementAt(i);

      StringTokenizer st = new StringTokenizer(line);
      String special     = st.nextToken();
      String mount_point = st.nextToken();
      String fstype      = st.nextToken();
      String options     = st.nextToken();
      if (debug) common.ptod("mount_point scan: " + mount_point + " fstype: " + fstype);

      /* These two lines were added for Steven Hoeck 3/25/04.           */
      /* With the mount point being "/" (it never can be just "")       */
      /* the 'startswith' below would never match.                      */
      /* I can not imagine there ever having been a "//" match!         */
      /* All other mount points do NOT end with "/", therefore          */
      /* the 'startswith'.                                              */
      /* I don't think we ever found a match with use_mount_point set?? */
      if (mount_point.equals("/"))
        mount_point = "";

      //common.ptod("use_mount_point: " + use_mount_point);
      //common.ptod("mount_point.length(): " + mount_point.length() + " " + mount_point);

      // there's a bug here: having /henk1 and /henk2 mounted on different
      // volumes pick up the '/' mountpoint instead of their own.

      /* Ignore mount point '/': */
      if (!use_mount_point && mount_point.length() <= 1)
        continue;

      /* For some reason 'fname' gets translated to lowercase when doing   */
      /* rebuildNativePointers(). This below works around a case mismatch: */
      boolean starts = fname.toLowerCase().startsWith(mount_point.toLowerCase() + "/");
      if (debug) common.ptod("startsWith: " + starts);
      if (starts)
      {
        if (fstype.compareTo("nfs") == 0 ||
            fstype.compareTo("ufs") == 0 ||
            fstype.compareTo("zfs") == 0 ||
            fstype.compareTo("qfs") == 0 ||
            fstype.compareTo("samfs") == 0)
        {

          if (debug) common.ptod("line: " + line);
          st = new StringTokenizer(options, " ,");
          while (st.hasMoreTokens())
          {
            String token = st.nextToken();
            if (token.startsWith("dev="))
            {
              int devt  = Integer.parseInt(token.substring(4), 16);
              String instance = fstype + (devt & 0x3ffff);
              String out[] = { instance, special, fstype, mount_point};

              if (debug)
              {
                common.ptod("get_mnttab: fname: " + fname +
                            " mountpoint: " + mount_point +
                            " instance: " + instance +
                            " special: " + special +
                            " fstype: "  + fstype);
              }
              return out;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Unused: recursively list a directory
   */
  public static void not_used_list_dir(File parent, String child)
  {

    //list_dir(new File("/"), "dev/rdsk");

    File newdir = null;
    newdir = new File(parent, child);

    if (parent != null)
      common.ptod("xListing directory: " + parent.getAbsolutePath() + System.getProperty("file.separator") + child);
    String filenames[] = newdir.list();

    /* If there is something to delete, delete it: */
    if (filenames != null)
    {
      for (int i = 0; i < filenames.length; i++)
      {
        File f = new File(newdir, filenames[i]);
        if (f.isDirectory())
        {
          not_used_list_dir(newdir, filenames[i]);
        }
        else
        {
          File newfile = new File(parent, filenames[i]);
          common.ptod(parent.getAbsolutePath() +
                      System.getProperty("file.separator") + child +
                      System.getProperty("file.separator") + filenames[i] +
                      " length: " + newfile.length());
          try
          {
            common.ptod(parent.getCanonicalPath() +
                        System.getProperty("file.separator") + child +
                        System.getProperty("file.separator") + filenames[i] +
                        " length: " + newfile.length());
          }
          catch (Exception e)
          {
            common.failure(e);
          }
        }
      }
    }
  }



  /**
   * Read /etc/mnttab and sort it on the mount point.
   *
   * We must first sort this list by mountpoint to avoid a problem where:
   * /export/home   is found BEFORE the more specific
   * /export/home/jimb/thumper/zfs-1dsk
   */
  private static void readAndSortMnttab()
  {
    mnttab = Fget.read_file_to_vector("/etc/mnttab");


    /* We must first sort this list by mountpoint to avoid a problem where: */
    /* /export/home   is found BEFORE the more specific                     */
    /* /export/home/jimb/thumper/zfs-1dsk                                   */
    Vector new_list = new Vector(mnttab.size());
    while (mnttab.size() > 0)
    {
      String first_line = (String) mnttab.firstElement();
      //common.ptod("first_line: " + first_line);
      StringTokenizer st = new StringTokenizer(first_line);

      /* Ignore if the line does not look healthy: */
      if (st.countTokens() < 4)
      {
        common.plog("Short mnttab data1: " + first_line);
        mnttab.removeElementAt(0);
        continue;
      }
      st.nextToken();
      int lowest_index = 0;
      String lowest    = st.nextToken();

      /* Find the smallest: */
      for (int i = 0; i < mnttab.size(); i++)
      {
        String next_line = (String) mnttab.elementAt(i);
        //common.ptod("next_line: " + next_line);
        StringTokenizer st2 = new StringTokenizer(next_line);

        /* Ignore if the line does not look healthy: */
        if (st2.countTokens() < 4)
        {
          common.plog("Short mnttab data2: " + next_line);
          mnttab.removeElementAt(i);
          continue;
        }

        st2.nextToken();
        String current = st2.nextToken();
        if (current.compareTo(lowest) < 0)
          lowest_index = i;
      }

      new_list.addElement(mnttab.elementAt(lowest_index));
      mnttab.removeElementAt(lowest_index);
    }

    mnttab = new_list;
    if (debug)
    {
      for (int i = 0; i < mnttab.size(); i++)
        common.ptod("sorted: " + mnttab.elementAt(i));
    }
  }

  /**
   * Split a line into blank delimited fields in a String array
   */
  public static void split_line(String line)
  {
    int pos = 0;
    int begin = 0;
    int end = line.length();
    char cline[] = line.toCharArray();

    //System.out.println(line);

    col = 0;
    if (cline[0] == ' ')
    {
      while (pos < end && cline[pos] == ' ') pos++;
      begin = pos;
    }

    while (pos < end)
    {
      while (pos < end && cline[pos] != ' ') pos++;
      cols[col++] = line.substring(begin, pos);

      //System.out.println("data: " + cols[col-1]);

      while (pos < end && cline[pos] == ' ')
      {
        //System.out.println(pos + " pos1: " + line.charAt(pos));
        pos++;
      }

      begin = pos;
    }
  }



  /**
   * get libdevinfo data from Solaris.
   *
   * libdevinfo is no longer used; instead a merging of ls -l and iostat is used.
   */
  public static void getDeviceLookupData()
  {
    int i;

    /* Get strings with all 'ssdxx ssdxx,g major minor' */
    Vector libdev = InstanceXlate.simulateLibdev();

    /* Take all these devices and put then in the instance_list Vector: */
    for (int x = 0; x < libdev.size(); x++)
    {
      String line = (String) libdev.elementAt(x);
      //common.ptod("line: " + line);

      // /dev/rdsk/c0t1d0               ssd0          118 0 indx
      // /dev/rdsk/c0t1d0s2             ssd0,c        118 2 ends
      StringTokenizer st = new StringTokenizer(line);
      String path        = st.nextToken();
      String instance    = st.nextToken();
      int    major       = 0; //Integer.valueOf(st.nextToken()).intValue();
      int    minor       = 0; //Integer.valueOf(st.nextToken()).intValue();

      /* To make sure that we only use whole disks (no slice) */
      /* ignore the instances that contain a comma:           */
      if (instance.indexOf(",") != -1)
        continue;

      Devxlate devx = new Devxlate(path);
      devx.instance = instance;
      instance_list.add(devx);
      if (debug) common.ptod("getDeviceLookupData added: " + devx.fullname);
    }
  }



  /**
   * Translate file name to (list of) volumes and instance names
   * where the file resides.
   *
   * We create a Vector of Strings. Each time we drill a little deeper in
   * the device translation we add the new real or volume manager names
   * at the end of this Vector.
   * We keep scanning until we have found them all.
   *
   * The returned Vector can contain error message Strings to explain why we
   * failed to get the proper Kstat info.
  */
  public static Vector get_device_info(String fname_in)
  {
    if (debug) common.ptod("get_device_info: " + fname_in);

    /* Set up start of search list: */
    search_list = new Vector(64,0);
    add_to_search(new File(fname_in).getAbsolutePath());

    Vector dlist = new Vector(64,0);

    /* Recursively search through list: */
    for (int i = 0; i < search_list.size(); i++)
    {
      String fullname = (String) search_list.elementAt(i);
      if (debug) common.ptod("search: " + fullname);

      if (fullname.startsWith("/dev/zvol"))
      {
        StringTokenizer st = new StringTokenizer(fullname, "/");
        if (st.countTokens() == 5)
        {
          st.nextToken();
          st.nextToken();
          st.nextToken();
          findZfsDevices(st.nextToken());
        }
      }

      /* Look for anything that is not a real disk: */
      if (!fullname.startsWith("/dev/dsk/") &&
          !fullname.startsWith("/dev/rdsk/") &&
          !fullname.startsWith("/dev/rmt/") )
      {

        if (fullname.startsWith("/dev/vx/dsk/") ||
            fullname.startsWith("/dev/vx/rdsk/") )
        {
          get_vxvm_list(fullname);
          search_list.setElementAt(null, i);
        }

        /* SDS disks get picked up using the 'metastat' command: */
        //else if (fullname.startsWith("/dev/md/dsk/") ||
        //         fullname.startsWith("/dev/md/rdsk/") )
        else if (fullname.startsWith("/dev/md/") ||
                 fullname.startsWith("/dev/md/") )
        {
          get_sds_list(fullname);
          search_list.setElementAt(null, i);
        }

        else if (get_vfstab(fullname))
        {
          //continue;
        }

        else
        {
          /* See if this is a mounted file system [instance,special,fstype]: */
          String out[] = get_mnttab(fullname, false);
          if (out == null)
          {
            out = get_mnttab(fullname, true);
            if (out == null)
            {
              kstat_error(fullname);
              dlist.add("get_device_info(): could not find: " + fullname);
              return dlist;
            }
          }

          String instance = out[0];
          String special  = out[1];
          String fstype   = out[2];

          if (debug) common.ptod("gdi: " + instance + " " + special + " " + fstype);

          /* For a mounted SDS volume put volume names in the list: */
          if (special.startsWith("/dev/md"))
            add_to_search(special);

          /* VXVM: */
          else if (special.startsWith("/dev/vx"))
            add_to_search(special);

          /* Samfs is also a special case: */
          else if ( fstype.equals("samfs") || fstype.equals("qfs") )
            get_qfs_list(special);

          else if (fstype.equals("ufs"))
          {
            /* UFS. we need to search for the 'special' disk name: */
            dlist.add(scan_fullname(special));
          }


          /* ZFS: assume for now that we can always pick */
          /* the very first part of special:             */
          else if (fstype.equals("zfs"))
          {
            String pool;
            if (special.indexOf("/") != -1)
              pool = special.substring(0, special.indexOf("/"));
            else
              pool = special;

            findZfsDevices(pool);
          }

          else
          {
            /* It is just a regular mounted file system (ufs/nfs) */
            /* No further searching needed, add straight to dlist: */
            if (debug)
              System.out.println("adding from mnttab: " + instance + "  " + special +
                                 "  " + fstype + "  " + fullname);
            dlist.add(add_instance_to_list(instance, fullname, special));
          }
          search_list.setElementAt(null, i);
        }
      }
    }


    /* The search list now only has raw device names left.              */
    /* See if you can find these names in the list that was created     */
    /* from the 'ls /dev/' command.                                     */

    /* (You can have any amount of raw devices in the search list left  */
    /* because of volume managers above adding all raw devices involved */
    /* just for the single requested file name or volume manager volume */
    /* entered at the top of this search.)                              */
    for (int i = 0; i < search_list.size(); i++)
    {
      String name = (String) search_list.elementAt(i);
      if (name != null)
      {
        if (debug) common.ptod("search_list name: " + name);
        Object devx = scan_fullname(name);
        if (devx != null)
          dlist.add(devx);
      }
    }

    if (dlist.size() == 0)
    {
      kstat_error(fname_in);
      dlist.add("Unable to find Kstat information for " + fname_in);
      return dlist;
    }

    return dlist;
  }


  /**
   * Add a filename/diskname to search list
   */
  private static void add_to_search(String newname)
  {
    if (debug) common.ptod("add_to_search(): " + newname);

    for (int i = 0; i < search_list.size(); i++)
    {
      String name = (String) search_list.elementAt(i);
      if (name != null && newname.compareTo(name) == 0)
        return;
    }

    if (debug)
    {
      if (search_list.size() == 0)
        common.plog("Kstat search: starting search for: " + newname);
      else
        common.plog("Kstat search: adding device: " + newname);
    }

    search_list.addElement(newname);
  }


  /**
   * Get vxprint list   vxprint -ht -g petedg testvol
   */
  private static void get_vxvm_list(String fname)
  {
    Vector vxprint = new Vector(64,0);
    Vector vxdisk  = new Vector(64,0);
    Vector stderr  = new Vector(64,0);
    String q1, q2, q3, group, volume, temp;
    String fullname = null;
    Vector names = new Vector(64,0);
    names.addElement(fname);

    try
    {

      while (names.size() > 0)
      {
        fullname = (String) names.firstElement();
        names.removeElementAt(0);

        /* Separate group and volume name: */
        if (debug)
          common.ptod("get_vxvm_list() looking for: " + fullname);
        StringTokenizer st = new StringTokenizer(fullname, "/");
        if (st.countTokens() < 4)
          return;
        q1     = st.nextToken();
        q2     = st.nextToken();
        q3     = st.nextToken();
        group  = st.nextToken();

        /* Experiment: if group is not used, default to 'rootdg': */
        if (st.hasMoreTokens())
          volume = st.nextToken();
        else
        {
          volume = group;
          group = "rootdg";
        }

        /* List the group + volume: */
        os_command("vxprint -ht -g " + group + " " + volume, vxprint, stderr);

        /* Read all lines: */
        for (int i = 0; i < vxprint.size(); i++)
        {
          String line = (String) vxprint.elementAt(i);
          common.plog("vxprint: " + line);

          if (!line.startsWith("sd") && !line.startsWith("s2") && !line.startsWith("sv"))
            continue;

          /* Column 4 contains the volume name: */
          st = new StringTokenizer(line);
          if (st.countTokens() < 4)
            continue;
          volume = st.nextToken();
          volume = st.nextToken();
          volume = st.nextToken();
          volume = st.nextToken();

          if (line.startsWith("sv"))
          {
            if (debug) common.ptod("adding for vxprint: " + volume);
            names.addElement(q1 + "/" + q2 + "/" + q3 + "/" + group + "/" + volume);
            continue;
          }

          /* If this disk name is known, use it otherwise go to vxdisk: */
          if (scan_fullname("/dev/rdsk/" + volume) != null)
            add_to_search("/dev/rdsk/" + volume);
          else
          {

            /* We now have the encapsulated volume name. Call vxdisk: */
            os_command("vxdisk -g " + group + " list " + volume, vxdisk, stderr);

            /* Read all lines: */
            for (int j = 0; j < vxdisk.size(); j++)
            {
              line = (String) vxdisk.elementAt(j);
              common.plog("vxdisk: " + line);

              /* 'numpaths:' has the amount of paths: */
              if (!line.startsWith("numpaths:"))
                continue;
              st = new StringTokenizer(line);
              if (st.countTokens() < 2)
                continue;
              st.nextToken();
              int paths = Integer.valueOf(st.nextToken()).intValue();

              for (int k = 0; k < paths; k++)
              {
                line = (String) vxdisk.elementAt(++j);
                common.plog("numpaths: " + line);
                st = new StringTokenizer(line);

                /* Per Torrey: All VXVM data is stored under slice 4: */
                String device = st.nextToken();
                device = device.substring(0, device.lastIndexOf("s")) + "s4";
                add_to_search("/dev/rdsk/" + device);
              }
            }
          }
        }
      }
    }
    catch (Exception e)
    {
      common.ptod("error while searching for VXVM volume '" + fullname + "'");
      common.ptod("Expecting volume name in the format of '/dev/vx/rdsk/group/volume'");
      kstat_error(fname);
    }
  }



  /**
   * Get QFS device list from /etc/opt/LSCsamfs/mcf or /etc/opt/SUNWsamfs/mcf
   *
   * fyi (there is a sam_stat() function, don't know if it is useful)
   */
  private static void get_qfs_list(String special)
  {

    /* List the mcf: */
    if (mcf == null)
    {
      if (new File("/etc/opt/SUNWsamfs/mcf").exists())
        mcf = Fget.read_file_to_vector("/etc/opt/SUNWsamfs/mcf");
      else if (new File("/etc/opt/LSCsamfs/mcf").exists())
        mcf = Fget.read_file_to_vector("/etc/opt/LSCsamfs/mcf");
    }

    /* Read all lines, ignore comments: */
    for (int i = 0; i < mcf.size(); i++)
    {
      String line = (String) mcf.elementAt(i);
      if (line.startsWith("#"))
        continue;

      /* Column 4 contains the file system name: */
      StringTokenizer st = new StringTokenizer(line);
      if (st.countTokens() < 4)
        continue;
      String col1 = st.nextToken();
      String col2 = st.nextToken();
      String col3 = st.nextToken();
      String col4 = st.nextToken();

      //common.ptod("special : " + special );
      //common.ptod("col1 : " + col1 );
      //common.ptod("col2 : " + col2 );
      //common.ptod("col3 : " + col3 );
      //common.ptod("col4 : " + col4 );

      /* If this is the file system, pick up the disks: */
      if (col4.compareTo(special) == 0)
      {
        if (col1.compareTo(special) != 0)
        {
          add_to_search(col1);
        }
      }
    }
  }


  /**
   * Get SDS device list.
   *
   * Here is what we scan: we look for the first word in any line that looks
   * like CxTxDxSx
   *
   */
  private static void get_sds_list(String fullname)
  {
    OS_cmd ocmd = new OS_cmd();
    String[] metastat;
    String[] stderr;
    String metadevice, diskgroup;
    String line;

    // /dev/md/testset/dsk/d20

    //common.ptod("get_sds_list: " + fullname);

    /* Get the sds device: */
    StringTokenizer st = new StringTokenizer(fullname, "/");
    if (st.countTokens() < 4)
      common.failure("get_sds_list() No meta device name in /dev/md/: " + fullname);

    if (st.countTokens() == 4)
    {
      st.nextToken();
      st.nextToken();
      st.nextToken();
      metadevice = st.nextToken();

      ocmd.addText("/usr/sbin/metastat");
      ocmd.addText(metadevice);
      ocmd.execute();
      //os_command("/usr/sbin/metastat " + metadevice, metastat, stderr);
    }
    else
    {
      st.nextToken();
      st.nextToken();
      diskgroup  = st.nextToken();
      st.nextToken();
      metadevice = st.nextToken();

      ocmd.addText("/usr/sbin/metastat -s");
      ocmd.addText(diskgroup);
      ocmd.addText(metadevice);
      ocmd.execute();
      //os_command("/usr/sbin/metastat -s " + diskgroup + " " + metadevice, metastat, stderr);
    }

    metastat = ocmd.getStdout();
    stderr   = ocmd.getStderr();

    if (stderr.length == 0)
    {
      /* Look for line starting with cxtxdxsx: */
      for (int i = 0; i < metastat.length; i++)
      {
        line = metastat[i].trim();
        if (does_this_contain_ctds(line))
        {
          st = new StringTokenizer(line);
          String diskname = st.nextToken();

          /* I had an instance where the disk name already contained /dev/: */
          if (diskname.startsWith("/dev/rdsk/"))
            diskname = diskname.substring(10);
          else if (diskname.startsWith("/dev/dsk/"))
            diskname = diskname.substring(9);

          add_to_search("/dev/rdsk/" + diskname);
        }
      }
    }

    return;

  }

  private static boolean device_start_block(String line)
  {
    return( line.toLowerCase().indexOf("device") != -1 &&
            line.toLowerCase().indexOf("start")  != -1 &&
            line.toLowerCase().indexOf("block")  != -1);
  }


  /**
   * Determine if the syntax of some text is CxTxDxSx
   */
  static boolean does_this_contain_ctds(String line)
  {
    StringTokenizer st = new StringTokenizer(line);

    /* Get the first value from the line: */
    if (!st.hasMoreTokens())
      return false;
    String token = st.nextToken().toLowerCase();

    /* Look for ctds in this order: */
    int c = token.indexOf('c');
    int t = token.indexOf('t');
    int d = token.lastIndexOf('d');
    int s = token.indexOf('s');

    //common.ptod(token);
    //common.ptod("c t d s " + c + " " + t + " " + d + " " + s);

    if (c < 0 || t < 0 || d < 0)    //   || s < 0)
      return false;

    if ( (c < t) && (t < d)) // && (d < s))
      return true;

    return false;
  }


  /**
   * we found an nfs/ufs instance name. Add to list
   */
  private static Devxlate add_instance_to_list(String instance,
                                               String fullname,
                                               String special)
  {
    /* If the fullname is already there just return the old one: */
    for (int i = 0; i < instance_list.size(); i++)
    {
      Devxlate devx = (Devxlate) instance_list.elementAt(i);
      if (devx.fullname.equalsIgnoreCase(fullname))
        return devx;
    }

    Devxlate devx      = new Devxlate(fullname);
    devx.instance      = instance;
    devx.special       = special;

    if (debug)
      common.ptod("add_instance_to_list: " + devx.fullname);

    //common.ptod("adding devxlate for2: " + devx.fullname);
    instance_list.addElement(devx);

    return devx;
  }

  public static void removeFromInstanceList(String fullname)
  {
    for (int i = 0; i < instance_list.size(); i++)
    {
      Devxlate devx = (Devxlate) instance_list.elementAt(i);
      if (devx.fullname.equalsIgnoreCase(fullname))
      {
        instance_list.removeElementAt(i);
        break;
      }
    }
  }


  /**
   * Scan devices list for a full disk name.
   * Starting Vdbench500 we remove slice numbers. This is done to finally go
   * around those systems where one or more slices are missing from kstat.
   */
  private static Object scan_fullname(String fullname)
  {
    if (debug) common.ptod("scan_fullname(): " + fullname);

    /* Remove the slice number from this fullname: */
    DevicePieces dp = new DevicePieces(fullname);
    fullname = dp.getNameNoSlice();

    for (int i = 0; i < instance_list.size(); i++)
    {
      Devxlate devx = (Devxlate) instance_list.elementAt(i);
      if (debug) common.ptod("scan_fullname fullname: " + devx.fullname);

      if (fullname.endsWith("/c1t0d0") && common.get_debug(common.FORCE_KSTAT_ERROR))
      {
        return "Forcibily returning a Kstat failure for debugging: " + fullname;
      }

      if (fullname.equalsIgnoreCase(devx.fullname))
      {
        if (debug) common.ptod("scan_fullname(): found " + fullname + " " + devx.instance);
        return devx;
      }
    }

    return null; //"Unable to find proper Kstat information for " + fullname;
  }



  /**
   * There is a problem translating device or file names to raw devices and
   * therefore Kstat instances.
   * Write some warning messages, but continue.
   * Notify the master that one or more devices are missing
   */
  static void kstat_error(String txt)
  {
    common.ptod("Kstat error: " + txt);
    common.ptod("The following stack trace serves only for extra diagnostics for the message that is following:");
    Throwable th = new Throwable();
    th.printStackTrace(common.stdout);
    th.printStackTrace(common.log_html);

    common.ptod("");
    common.ptod("");
    common.ptod("Unable to retrieve native kstat information for " + txt + ".");

    /* Don't clutter up the screen while debugging: */
    if (!debug)
    {
      common.ptod("This kstat information is needed to collect proper iostat-like data.");
      //common.ptod("(This problem  can happen when a volume does not have a label)");
      //common.ptod("Please contact author of vdbench for assistance with resolving this problem.");

      common.ptod("vdbench will continue without collecting kstat statistics.");
      common.ptod("");
      common.ptod("");

      InstanceXlate.printIostat();


      if (Vdbmain.kstat_console)
      {
        common.ptod("Using execution parameter '-k' without kstat being active.");
        common.ptod("The result is that no output would be reported on the console!");
        common.ptod("The '-nk' parameter has been reset and execution continues. ");
        Vdbmain.kstat_console = false;
      }
    }

    /* Make sure that master is warned. This info is picked up by InfoFromHost() */
    SlaveJvm.setKstatError();

    //Kstat.needed = false;
    //Kstat.suppressed = true;
  }


  /**
   *
   * # zpool status pool-1dsk
   *   pool: pool-1dsk
   *  state: ONLINE
   *  scrub: none requested
   * config:
   *
   *         NAME        STATE     READ WRITE CKSUM
   *         pool-1dsk   ONLINE       0     0     0
   *           c0t0d0s2  ONLINE       0     0     0
   *
   * errors: No known data errors
   *
   */

  private static void findZfsDevices(String pool)
  {
    /* Don't want to waste any time: */
    if (!pool.equals(last_zpool_checked))
    {
      last_zpool_checked = pool;
      OS_cmd ocmd = new OS_cmd();
      ocmd.addText("/usr/sbin/zpool status  " + pool);
      ocmd.execute();

      if (debug) common.ptod("findZfsDevices: " + pool);

      zfs_stdout = ocmd.getStdout();

      // This is just debugging. Removed.
      //ocmd = new OS_cmd();
      //ocmd.addText("/usr/sbin/zfs get all");
      //ocmd.addText(pool);
      //ocmd.execute();
      //String[] stdout = ocmd.getStdout();
      //String[] stderr = ocmd.getStderr();
      //for (int i = 0; i < stdout.length; i++)
      //  common.ptod("stdout: " + stdout[i]);
      //for (int i = 0; i < stderr.length; i++)
      //  common.ptod("stderr: " + stderr[i]);
    }


    int index = 0;

    /* Skip until I find the pool name: */
    for (index = 0; index < zfs_stdout.length; index++)
    {
      String line        = zfs_stdout[index];
      StringTokenizer st = new StringTokenizer(line);
      if (!st.hasMoreTokens())
        continue;
      if (st.nextToken().equals(pool))
      {
        index++;
        break;
      }
    }


    /* After this we find device names until we get a line with "errors": */
    for (; index < zfs_stdout.length; index++)
    {
      String line = zfs_stdout[index];
      if (line.startsWith("errors"))
        break;

      StringTokenizer st = new StringTokenizer(line);
      if (!st.hasMoreTokens())
        continue;
      String device = st.nextToken();

      /* There is some garbage possible: */
      if (device.equalsIgnoreCase("raidz") ||
          device.equalsIgnoreCase("mirror"))
        continue;

      /* If there's no slice, add one: */
      // it appears that once I made the decision to no longer look for slices
      // that this 'adding s0' was obsolete??
      if (false && device.indexOf("s") == -1)
      {
        add_to_search("/dev/rdsk/" + device + "s0");
        //add_to_search("/dev/rdsk/" + device + "s8");
        if (debug) common.ptod("ZFS 1 added to search: " + device + "s0");
        //common.ptod("ZFS added to search: " + device + "s8");
      }

      else if (!device.startsWith("/dev/"))
      {
        add_to_search("/dev/rdsk/" + device);
        if (debug) common.ptod("ZFS 2 added to search: " + device);
      }

      else
      {
        add_to_search(device);
        if (debug) common.ptod("ZFS 3 added to search: " + device);
      }
    }
  }



  public static void main(String[] args)
  {
  }
}
