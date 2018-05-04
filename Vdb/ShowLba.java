package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.awt.*;
import java.awt.event.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.*;
import java.util.*;

import javax.imageio.ImageIO;
import javax.swing.*;
import javax.swing.border.EtchedBorder;

import Utils.Fget;
import Utils.Fput;
import Utils.Getopt;

/**
 * GUI to display the resolts of 'CREATE_FAKE_TRACE', debug=63.
 * This option creates one file per slave with the results of its i/o.
 *
 * ./vdbench Vdb.ShowLba output/localhost-0.faketrace.txt
 *
 * (Should probably allow concatenation of all trace files?)
 */
public class ShowLba extends JPanel implements ActionListener
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private Tframe frame = new Tframe(String.format("%150s", "ShowLba"));
  private ArrayList <TodLba> data = new ArrayList(32768);
  private long reads = 0;
  private long writes = 0;


  private long  seek_width;
  private long  duration;

  private long min_tod = Long.MAX_VALUE;
  private long max_tod = 0;
  private long min_lba = Long.MAX_VALUE;
  private long max_lba = 0;

  JLabel legend = new JLabel();
  JLabel label  = new JLabel();

  private ArrayList <String> sds_found = new ArrayList(8);
  public static Color[] colors = new Color[]
  {
    new Color(0,   0,   255),  //   0
    new Color(255, 0,   0  ),  //   1
    new Color(0,   255, 0  ),  //   2
    new Color(0,   255, 255),  //   3
    new Color(255, 200, 0  ),  //   4
    new Color(255, 175, 175),  //   5
    new Color(255, 255, 0  ),  //   6
    new Color(255, 0,   255),  //   7
    new Color(192, 192, 192),  //   8
    new Color(64,  64,  64 ),  //   9
    new Color(0,   0,   178),  //  10
    new Color(178, 0,   0  ),  //  11
    new Color(0,   178, 0  ),  //  12
    new Color(0,   178, 178),  //  13
    new Color(178, 140, 0  ),  //  14
    new Color(178, 122, 122),  //  15
    new Color(178, 178, 0  ),  //  16
    new Color(178, 0,   178),  //  17
    new Color(134, 134, 134),  //  18
    new Color(0,   0,   0  )   //  19
  };

  private int  dot_size   = 1;


  private JButton exit     = new JButton("Exit");
  private JButton refresh  = new JButton("Refresh");
  private JButton bigdot   = new JButton("Big dots");
  private JButton smalldot = new JButton("Small dots");


  private static Fput fake_trace = null;
  private static long trace_start = 0;  /* In milliseconds */


  public ShowLba()
  {
  }

  public void doit()
  {
    frame.setTitle("Vdb.ShowLba");
    frame.setSize(1200, 600);

    frame.setLayout(new GridBagLayout());


    exit.addActionListener(this);
    bigdot.addActionListener(this);
    smalldot.addActionListener(this);

    /*
    *   1 @param gridx      The initial gridx value.
    *   2 @param gridy      The initial gridy value.
    *   3 @param gridwidth  The initial gridwidth value.
    *   4 @param gridheight The initial gridheight value.
    *   5 @param weightx    The initial weightx value.
    *   6 @param weighty    The initial weighty value.
    *   7 @param anchor     The initial anchor value.
    *   8 @param fill       The initial fill value.
    *   9 @param insets     The initial insets value.
    *  10 @param ipadx      The initial ipadx value.
    *  11 @param ipady      The initial ipady value.
    */

    Container cp = frame.getContentPane();
    cp.add(exit,     new GridBagConstraints(0, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    cp.add(bigdot,   new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    cp.add(smalldot, new GridBagConstraints(2, 0, 1, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    cp.add(legend,   new GridBagConstraints(0, 1, 3, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    cp.add(this,     new GridBagConstraints(0, 2, 3, 1, 1.0, 1.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));
    cp.add(label,    new GridBagConstraints(0, 3, 3, 1, 1.0, 0.0, GridBagConstraints.CENTER, GridBagConstraints.BOTH, new Insets(0, 0, 0, 0), 0, 0));


    Message.centerscreen(frame);
  }

  public boolean addLba(String line)
  {
    try
    {
      TodLba tl = new TodLba();

      // sd1 wd1 0 3345408 102
      String[] split = line.trim().split(" +");
      //addLba(split[0], Long.parseLong(split[1]), Long.parseLong(split[2]));

      tl.sd     = split[0];
      tl.wd     = split[1];
      tl.read_flag = (split[2].charAt(0) == '1');
      tl.lba    = Long.parseLong(split[3]);
      tl.tod    = Long.parseLong(split[4]);

      min_tod = Math.min(min_tod, tl.tod);
      max_tod = Math.max(max_tod, tl.tod);
      min_lba = Math.min(min_lba, tl.lba);
      max_lba = Math.max(max_lba, tl.lba);


      /* Each SD gets his own color: */
      int color_index = sds_found.indexOf(tl.sd);
      if (color_index < 0)
      {
        sds_found.add(tl.sd);
      }
      color_index = sds_found.indexOf(tl.sd);

      tl.color = colors[color_index];

      data.add(tl);
      if (tl.read_flag)
        reads++;
      else
        writes++;
      return true;
    }
    catch (Exception e)
    {
      common.ptod(e);
      common.ptod("Error parsing input. Setting to EOF");
      return false;
    }

  }


  private static void usage()
  {
    //Getopt getopt = new Getopt(args, "w:s:d:o:", 99);
    common.ptod("Usage: " );
    common.ptod("./vdbench Vdb.ShowLba [-w wdname] [-s sdname] [-o out.jpg]");

    common.failure("parameter error");
  }

  public static void main(String[] in_args)
  {
    /* Remove the first argument passed from Vdbmain: */
    String[] args = new String[ in_args.length - 1];
    System.arraycopy(in_args, 1, args, 0, args.length);

    Getopt getopt = new Getopt(args, "w:s:d:o:", 99);
    getopt.print("ShowLba");
    if (!getopt.isOK() || getopt.get_positionals().size() == 0)
    {
      common.ptod("Usage: ./vdbench showlba [-w WDname] [-s SDname] [-o output.png] localhost-?.showlba.txt");
      common.ptod("Where: ");
      common.ptod("     -s SDname:       Filter looking for sd=SDname");
      common.ptod("     -w WDname:       Filter looking for wd=WDname");
      common.ptod("     -o output.png    Do not display, create PNG file instead.");
      common.ptod("     localhost-0.showlba.txt  . . . .  trace file created by showlba=yes");

      common.failure("parameter error");
    }

    String parent    = null;
    String sd_search = null;
    String wd_search = null;
    if (getopt.check('s'))
      sd_search = getopt.get_string();
    if (getopt.check('w'))
      wd_search = getopt.get_string();

    ShowLba hbs = new ShowLba();


    for (String fname : getopt.get_positionals())
    {
      parent  = new File(fname).getAbsolutePath();
      parent  = new File(parent).getParentFile().getAbsolutePath();
      Fget fg = new Fget(fname);
      String line = null;

      try
      {
        while ((line = fg.get()) != null)
        {
          // sd1 wd1 0 3345408 102
          String[] split = line.trim().split(" +");
          String sd      = split[0];
          String wd      = split[1];
          if (sd_search != null && !sd.equals(sd_search))
            continue;
          if (wd_search != null && !wd.equals(wd_search))
            continue;
          if (!hbs.addLba(line))
            break;
        }
        fg.close();
      }
      catch (Exception e)
      {
        common.ptod("Exception reading ShowLba file, accepted");
        common.ptod(e);
        continue;
      }
    }

    hbs.doit();

    if (getopt.check('o'))
      savePanel(hbs, getopt.get_string(), new Dimension(1200, 600));
    else
      hbs.frame.setVisible(true);

  }

  public void actionPerformed(ActionEvent e)
  {
    if (e.getSource() == exit)
      System.exit(0);

    else if (e.getSource() == smalldot)
    {
      dot_size = 1;
      this.repaint();
    }

    else if (e.getSource() == bigdot)
    {
      dot_size = 2;
      this.repaint();
    }

  }

  public void paint(Graphics g)
  {
    boolean debug = false;

    g.setColor(getBackground());
    g.fillRect (0, 0, getWidth(), getHeight());

    //if (data.size() < 10)
    //{
    //  common.ptod("Not enough data to display; only %d data points.", data.size());
    //  System.exit(0);
    //}

    seek_width = max_lba - min_lba;
    //seek_width = max_lba;
    if (debug) common.ptod("seek_width: " + seek_width);

    duration = max_tod - min_tod;
    if (debug) common.ptod("duration: " + duration);

    Dimension size       = getSize();
    int width            = (int) size.getWidth();
    int height           = (int) size.getHeight();
    if (debug) common.ptod("width: " + width);
    if (debug) common.ptod("height: " + height);

    long bytes_per_pixel = seek_width / width;
    if (debug) common.ptod("bytes_per_pixel: " + bytes_per_pixel);

    long usecs_per_pixel = duration / height;
    if (debug) common.ptod("usecs_per_pixel: " + usecs_per_pixel);

    String txt = String.format("Lowest lba: %,d (%,dmb) (%,dgb); "+
                               "Highest lba: %,d (%,dmb) (%,dgb); "+
                               "number of ios: %,d; duration: %.2f seconds; "+
                               "reads: %,d; writes: %,d ",
                               min_lba, min_lba / 1048576l, min_lba / (1048576l * 1024),
                               max_lba, (max_lba / 1048576), (max_lba / (1048576l * 1024)),
                               data.size(),
                               duration / 1000.,
                               reads, writes);
    label.setBorder(new EtchedBorder(EtchedBorder.RAISED,Color.white,new Color(178, 178, 178)));
    label.setText(txt);

    legend.setForeground(Color.RED);
    legend.setBorder(new EtchedBorder(EtchedBorder.RAISED,Color.white,new Color(178, 178, 178)));

    Collections.sort(sds_found);
    String txt2 = "  Legend: Vertical axis: time, Horizontal axis: lba;  Each SD has its own color; SDs found: ";
    for (String sd : sds_found)
      txt2 += sd + " ";
    txt2 += " for help, run ./vdbench showlba -h";
    legend.setText(txt2);


    int pixels = 0;
    for (int i = 0; i < data.size(); i++)
    {
      TodLba tl    = data.get(i);
      int    y     = (int) ((tl.tod - min_tod) / usecs_per_pixel);
      int    x     = (int) ((tl.lba - min_lba) / bytes_per_pixel);
      g.setColor(tl.color);
      g.fillRect(x, y, dot_size, dot_size);
      pixels++;
      //common.ptod("dot_size: %4d %4d", x, y);
    }
    if (debug) common.ptod("pixels: " + pixels);
  }


  public static synchronized void openTrace()
  {
    if (!Validate.showLba())
      return;
    if (fake_trace == null)
    {
      fake_trace  = new Fput(SlaveJvm.getSlaveLabel() + ".showlba.txt");
      trace_start = System.currentTimeMillis();
    }
  }
  public static synchronized void closeTrace()
  {
    if (fake_trace != null)
    {
      fake_trace.close();
      fake_trace = null;
    }
  }


  /**
   * Write fake trace record. Timestamp in relative milliseconds since start
   */
  public static synchronized void writeRecord(Cmd_entry cmd)
  {
    fake_trace.println("%s %s %d %d %d",
                       (!Validate.sdConcatenation()) ? cmd.sd_ptr.sd_name : cmd.concat_sd.sd_name,
                       cmd.cmd_wg.wd_name,
                       (cmd.cmd_read_flag) ? 1 : 0,
                       (!Validate.sdConcatenation()) ? cmd.cmd_lba : cmd.concat_lba,
                       System.currentTimeMillis() - trace_start);
  }


  class TodLba
  {
    Color  color;
    String sd;
    String wd;
    boolean read_flag;
    long   tod;
    long   lba;
  }


  /**
   * Save chart, requested by batch reporting.
   */
  public static void savePanel(ShowLba showlba, String fname, Dimension dim)
  {
    if (fname != null && !fname.endsWith(".png"))
      fname += ".png";

    try
    {
      showlba.setSize(dim);
      showlba.validate();

      BufferedImage image2 = new BufferedImage((int) showlba.getWidth(),
                                               (int) showlba.getHeight(),
                                               BufferedImage.TYPE_INT_RGB);
      Graphics2D g2        = image2.createGraphics();

      try
      {
        showlba.paint(g2);
      }
      catch (Exception e)
      {
        common.ptod(e);
        common.failure(e);
      }

      boolean rc = ImageIO.write(image2, "png", new File(fname));
    }

    catch (Exception e)
    {
      common.failure(e);
    }
  }
}




