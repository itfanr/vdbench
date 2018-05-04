package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;

import Utils.Fget;
import Utils.Format;
import Utils.Fput;


/**
 * Vdbench File System testing control file.
 *
 * The file starts with information indicating as to when this control
 * file was written to the last time: at the BEGIN of a run or at the END.
 *
 * File continues with those values that determine the directory and
 * file structure: depth, width, files, distribution and the file sizes.
 *
 * The file is then followed by directory information, and then by file
 * information.
 *
 *
 * Warning: since the introduction of fsd=xxx,shared=yes the control file is no
 * longer used, which means that a user CAN change the file structure without
 * being forced to do a format.
 * It was decided 3/9/2015 that it was not worth the effort and the risk to fix
 * it since this problem has been out there for already 4+ years!
 *
 *
 */
class ControlFile
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private FileAnchor anchor;
  private boolean[]  directory_status;
  private long[]     file_status;

  public  static String CONTROL_FILE = "vdb_control.file";



  public ControlFile(FileAnchor an)
  {
    anchor = an;
  }


  public void clearStatus()
  {
    directory_status = null;
    file_status = null;
  }

  public boolean exists()
  {
    return(new File(anchor.getAnchorName(), CONTROL_FILE).exists());
  }


  /**
   * Remember for the next run what the file structure is.
   *
   * For file status:
   * - 'n': file does not exist
   * - 'f': file exists and is full
   * - 'nnnnn': the current file size
   *
   * Later on in the file_status array this is what we'll get:
   * - 'n': will be -1
   * - 'f': will be -2
   * - 'nnnn': will be 'nnnn'
   */
  public void writeControlFile(boolean shared, boolean start)
  {
    String deb = "";
    //preserveOldControlFile();

    common.ptod("Writing control file for anchor=" +
                anchor.getAnchorName() + " at " +
                ((start) ? "start" : "end") + " of run.");
    int existing_files = 0;
    int existing_dirs = 0;

    Fput fp = new Fput(anchor.getAnchorName(), CONTROL_FILE);

    fp.println("when  " + ((start) ? "start" : "end") + " " + new Date());
    fp.println("depth " + anchor.depth);
    fp.println("width " + anchor.width);
    fp.println("files " + anchor.files);
    fp.println("dist  " + anchor.dist);
    for (int i = 0; i < anchor.filesizes.length; i ++)
      fp.println("size  " + (long) anchor.filesizes[i]);

    /* No need for file status when shared: */
    if (!shared)
    {
      Vector dirs = anchor.getDirList();
      fp.println("directory status " + dirs.size());

      /* Write the directory status: */
      for (int i = 0; i < dirs.size(); i++)
      {
        Directory dir = (Directory) dirs.elementAt(i);

        if (debug) deb =  " " + i + " " + dir.getFullName();
        if (dir.exist())
        {
          existing_dirs++;
          fp.println("y" + deb);
        }
        else
          fp.println("n" + deb);
      }

      /* Write the file status: */
      Vector files          = anchor.getFileList();
      int    sizes          = files.size();
      int    files_opened   = 0;
      long   existing_bytes = 0;
      long   total_req      = 0;
      long   size_opened    = 0;

      fp.println("file status " + sizes);
      for (int i = 0; i < sizes; i++)
      {
        FileEntry fe = (FileEntry) files.elementAt(i);

        //common.ptod("ControlFile.writeControlFile: %s %4b %8d %8d %4b",
        //            fe.getFullName(), fe.exists(), fe.getReqSize(), fe.getCurrentSize(),
        //            Fget.file_exists(fe.getFullName()));

        writeFileStatus(fp, fe, i, (i == sizes - 1));

        total_req += fe.getReqSize();
        if (fe.exists())
        {
          existing_files++;
          existing_bytes += fe.getCurrentSize();
          if (fe.getOpened())
          {
            files_opened++;
            size_opened += fe.getCurrentSize();
          }
        }
      }

      fp.close();

      /* Re-read the control file and create a checksum: */
      long checksum = 0;
      for (String line : Fget.readFileToArray(fp.getName()))
      {
        for (int i = 0; i < line.length(); i++)
          checksum += line.charAt(i);
      }

      /* Now add this checksum to the end: */
      fp = new Fput(fp.getName(), true);
      fp.println("checksum: %d ", checksum);
      fp.close();

      common.ptod("Completed control file for anchor=" +
                  anchor.getAnchorName() + " at " +
                  ((start) ? "start" : "end") + " of run. " +
                  " dirs: " + dirs.size() + "/" + existing_dirs +
                  (true ? (" files: " + sizes + "/" + existing_files +
                           " sizes: " + FileAnchor.whatSize(total_req) + "/" +
                           FileAnchor.whatSize(existing_bytes)) : "") +
                  "/" + files_opened + "/" + FileAnchor.whatSize(size_opened));

      anchor.reportSizes(existing_files, existing_dirs, existing_bytes,
                         files_opened, size_opened);
    }

  }


  /**
   * Write status of a FileEntry.
   * If the status is the same as the previous entry, write '= nnn' when the
   * status changes, or when EOF is coming up.
   */
  private FileEntry last_fe    = null;
  private int       duplicates = 0;
  private boolean   debug = false;
  private void writeFileStatus(Fput fp, FileEntry fe, int index, boolean last)
  {
    boolean duplicate = false;

    if (index == 0)
    {
      last_fe = null;
      duplicates = 0;
    }

    /* Is this the very last one? */
    if (last)
    {
      if (duplicates > 0)
        fp.println("= " + duplicates);
      fp.println(getFileStatus(fe, index));
      last_fe    = null;
      duplicates = 0;
      return;
    }

    /* Compare this new file with the previous entry: */
    duplicate = (!debug &&
                 last_fe != null &&
                 last_fe.exists() == fe.exists() &&
                 last_fe.isFull() == fe.isFull() &&
                 last_fe.getCurrentSize() == fe.getCurrentSize());

    if (false && last_fe != null && anchor.getAnchorName().indexOf("large") != -1)
      common.ptod("fe: " + index + " " + last_fe.exists() + "/" + fe.exists() + " " +
                  last_fe.isFull() + "/" + fe.isFull() + " " +
                  last_fe.getCurrentSize() + "/" + fe.getCurrentSize() + " "
                  + duplicate);

    /* Duplicate? */
    if (duplicate)
      duplicates++;

    /* This is no duplicate. Any old duplicates outstanding? */
    else
    {
      if (duplicates > 0)
        fp.println("= " + duplicates);
      fp.println(getFileStatus(fe, index));
      duplicates = 0;
    }

    last_fe = fe;
  }

  private String getFileStatus(FileEntry fe, int index)
  {
    String status;
    if (!fe.exists())
      status = "n";
    else if (fe.isFull())
      status = "f";
    else
      status = "" + fe.getCurrentSize();

    if (debug)
      status += " " + index + " " + fe.getFullName();

    return status;
  }

  private void preserveOldControlFile()
  {
    String fname = new File(anchor.getAnchorName(), CONTROL_FILE).getAbsolutePath();
    for (int i = 1; i < 30; i++)
    {
      if (!new File(fname + i).exists() && new File(fname).renameTo(new File(fname + i)))
      {
        common.ptod("Debugging: renamed Control File: " + fname + i);
        return;
      }
    }
  }


  /**
   * Compare contents of control file with current setting for this anchor.
   * It also loads existing directory and file status.
   *
   * Interesting observation, though I do not consider the user deliberately
   * deleting the control file 'normal operations': when the control file does
   * not exist when running format=no, this code just falls through without
   * errors.
   *
   * Next observation: during a format=yes the control file is written, and then
   * during deleteOldStuff() gets deleted again, but then at the end, correctly,
   * it is written again.
   */
  public void readControlFile(FwgEntry fwg)
  {
    String   when  = "'not in control file'";
    long     depth = 0;
    long     width = 0;
    long     files = 0;
    double[] sizes = new double[256];
    int      size_index = 0;
    String   dist  = null;
    String[] split = null;

    common.ptod("Reading control file for anchor=" + anchor.getAnchorName());

    /* Start with reading and verifying checksum: */
    long checksum = 0;
    boolean checksum_found = false;


    for (String line : Fget.readFileToArray(anchor.getAnchorName(), CONTROL_FILE))
    {
      if (line.startsWith("checksum:"))
      {
        long old_check = Long.parseLong(line.split(" +")[1]);
        if (checksum != old_check)
          common.failure(" Corruption in control file '%s' Checksums: %d/%d",
                         anchor.getAnchorName(), old_check, checksum);
        else
          checksum_found = true;
      }
      for (int i = 0; i < line.length(); i++)
        checksum += line.charAt(i);
    }

    if (!checksum_found)
      common.failure("No checksum found in control file for %s", anchor.getAnchorName());


    Fget fg = new Fget(anchor.getAnchorName(), CONTROL_FILE);
    String line = null;

    /* First get the major structure information: */
    while ((line = fg.get()) != null)
    {
      //common.ptod("line: " + line);
      split = line.split(" +");
      if (split[0].equals("when"))
        when = split[1];
      else if (split[0].equals("depth"))
        depth = Long.parseLong(split[1]);
      else if (split[0].equals("width"))
        width = Long.parseLong(split[1]);
      else if (split[0].equals("files"))
        files = Long.parseLong(split[1]);
      else if (split[0].equals("dist"))
        dist = split[1];
      else if (split[0].equals("size"))
        sizes[ size_index++ ]= Double.parseDouble(split[1]);
      else if (split[0].equals("directory"))
        break;
      else
        common.failure("Invalid value in control file: " + line + ">>" + split[0] + "<<");
    }

    //common.ptod("printSizes: " + printSizes(sizes, size_index));


    /* This all must be valid: */
    //common.ptod("when: " + when);
    if ((!when.equals("start") && !when.equals("end")) ||
        depth != anchor.depth ||
        width != anchor.width ||
        files != anchor.files ||
        !dist.equals(anchor.dist) ||
        !checkSizes(anchor.filesizes, sizes, size_index))
    {
      String txt = "";
      txt += "\n" + "";
      txt += "\n" + "fwd=" + fwg.getName();
      txt += "\n" + "when=" + when;
      txt += "\n" + "old depth=" + depth + "; new depth=" + anchor.depth;
      txt += "\n" + "old width=" + width + "; new width=" + anchor.width;
      txt += "\n" + "old files=" + files + "; new files=" + anchor.files;
      txt += "\n" + "old dist="  + dist  + "; new dist="  + anchor.dist;
      txt += "\n" + "also check the sizes=() parameters from previous and current execution.";
      txt += "\n" + "The FWD parameters defined for 'fwd=" + fwg.getName() + "' do not";
      txt += "\n" + "match the parameters used in the previous run. ";
      txt += "\n" + "- Correct the parameters, or";
      txt += "\n" + "- use the 'format=' RD parameter, or";
      txt += "\n" + "- Add '-c' execution parameter";
      txt += "\n" + "Make sure you also specify 'format=yes' in the Run Definition (RD)";
      common.ptod(txt);
      SlaveJvm.sendMessageToConsole(txt);
      common.failure("Parameter definition error");
    }

    /* The rest of the information may only be used if it was written at */
    /* the end of the previous run: */
    if (!when.equals("end"))
    {
      common.ptod("Anchor=" + anchor.getAnchorName() + ": Previous run did not "+
                  "complete. control file directory and file content will not be used");
      fg.close();
      return;
    }

    if (common.get_debug(common.NO_CONTROLFILE_DETAIL))
    {
      common.ptod("Anchor=" + anchor.getAnchorName() + ": Directory and file content"+
                  " from control file will not be used because of debug request");
      fg.close();
      return;
    }

    /* Ignore the rest of the CF when shared: */
    if (!fwg.shared)
    {
      /* Read the directory info: */
      directory_status = new boolean[ Integer.parseInt(split[2]) ];
      int dir_index = 0;
      while ((line = fg.get()) != null)
      {
        if (line.startsWith("file"))
          break;
        directory_status[dir_index++] = line.startsWith("y");
      }

      if (dir_index != directory_status.length)
        common.failure("Control file directory error. Expecting " +
                       directory_status.length + " but receiving " + dir_index);


      /* Read the file info: */
      split = line.split(" +");
      file_status = new long[ Integer.parseInt(split[2]) ];
      int file_index = 0;
      long last_status = -1;

      while ((line = fg.get()) != null)
      {
        //common.ptod("line: " + file_index + " " + line);
        /* Duplicates? If so, repeat the last status: */
        if (line.startsWith("="))
        {
          split = line.split(" +");
          int dups = Integer.parseInt(split[1]);
          for (int i = 0; i < dups; i++)
            file_status[file_index++] = last_status;
          continue;
        }

        else if (line.startsWith("n"))
          last_status = -1;
        else if (line.startsWith("f"))
          last_status = -2;
        else if (line.startsWith("checksum:"))
          continue;
        else
          last_status = Long.parseLong(line.trim().split(" +")[0]);

        /* Update the status: */
        file_status[file_index++] = last_status;
      }
      fg.close();

      if (file_index != file_status.length)
        common.failure("Control file file error. Expecting " +
                       file_status.length + " but receiving " + file_index);
    }

    common.ptod("Completed reading control file.");
  }


  private boolean checkSizes(double[] anchor_sizes,
                             double[] from_file,
                             int      size_index)
  {
    if (anchor_sizes.length != size_index)
      return false;

    for (int i = 0; i < size_index; i++)
    {
      if ((int) anchor_sizes[i] != (int) from_file[i])
        return false;
    }

    return true;
  }

  private String printSizes(double[] sizes, int size_index)
  {
    String txt = "(";
    for (int i = 0; i < size_index; i++)
      txt += (long) sizes[i] + ",";

    txt = txt.substring(0, txt.length() - 1) + ")";

    return txt;

  }

  public boolean hasDirStatus()
  {
    return directory_status != null;
  }
  public boolean hasFileStatus()
  {
    return(file_status != null);
  }

  public boolean getDirStatus(int index)
  {
    return directory_status[index];
  }


  /**
   * Return:
   * -1: file does not exists, or seqno is out of range
   * nn: file exists with length 'nn'
   */
  public long getFileSize(int seqno, long req_size)
  {
    /* If not within table, file does not exist: */
    if (seqno > file_status.length - 1)
      return -1;

    /* Does file exist? */
    if (file_status[seqno] == -1 )
      return -1;

    /* Is file full? */
    else if (file_status[seqno] == -2 )
      return req_size;

    /* Return current size: */
    else
      return file_status[seqno];
  }


  /**
   * rewrite all control files at the end of a run
   */
  public static void writeAllControlFiles()
  {
    Vector fwgs = SlaveWorker.work.fwgs_for_slave;
    HashMap anchor_map = new HashMap(16);
    boolean any_shared = false;
    for (int i = 0; i < fwgs.size(); i++)
    {
      FwgEntry fwg = (FwgEntry) fwgs.elementAt(i);
      anchor_map.put(fwg.anchor, fwg.anchor);
      if (fwg.shared)
      {
        any_shared = true;
        return;
      }
    }

    FileAnchor[] anchors = (FileAnchor[]) anchor_map.values().toArray(new FileAnchor[0]);

    for (int i = 0; i < anchors.length; i++)
    {
      anchors[i].getControlFile().writeControlFile(any_shared, false);
      anchors[i].clearControlFile();
    }
  }
}


