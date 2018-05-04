package Vdb;

/*
 * Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.*;
import java.util.*;


/**
 * This class handles data pattern information.
 */
public class Patterns implements Serializable
{
  private final static String c =
  "Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.";

  private String  pattern_file    = null;
  private int[]   pattern_array   = null;
  private boolean pattern_coded   = false;

  private static int print_once = 0;
  private static Patterns options = new Patterns();

  private static int buffer_size;
  private static int buffer_size_times2;

  /* This table gives you the randomizer values needed for compression.      */
  /* For instance, if you want a 1.26 compression ratio, look in the table   */
  /* until you find it or the next higher. Use the second value then to feed */
  /* into createCompressionPattern(). It uses those values to know how many  */
  /* random zeros to place in the buffer.                                    */
  static double[] randomizer_value = set7000Numbers();

  private static double default_compression = 1;


  public static double getDefaultCompressionRatio()
  {
    return default_compression;
  }
  public static void setDefaultCompressionRatio(double c)
  {
    default_compression = c;
    if (default_compression < 1)
      common.failure("Minimum value for compratio=%.0f is compratio=1",
                     default_compression);
  }


  public static Patterns getOptions()
  {
    return options;
  }
  public static void storeOptions(Patterns pat)
  {
    options = pat;
  }

  public static boolean usePatternFile()
  {
    return options.pattern_file != null;
  }

  public static int[] getPattern()
  {
    return options.pattern_array;
  }

  /**
   * Create the data pattern to use for write operations.
   * Minimum size: 1MB
   */
  public static void createPattern(int max_xfer)
  {
    /* This is done only once per run: */
    if (options.pattern_array != null)
      common.failure("createPattern(): unexpected call");

    /* Determine minimum data pattern buffer size: */
    buffer_size        = calculatePatternBufferSize(max_xfer);
    buffer_size_times2 = buffer_size * 2;
    //common.ptod("buffer_size_times2: " + buffer_size_times2);

    /* To accomodate IO_task's need for an extra long write buffer: */
    if (Validate.isDedup())
      buffer_size_times2 += Validate.getDedupUnit() * 2;

    /* No matter what, start with creating a compression pattern: */
    options.pattern_array = new int[buffer_size_times2 / 4];
    if (Validate.isCompression())
      createCompressionPattern(options.pattern_array, Validate.getCompressionRatio());
    else
      createCompressionPattern(options.pattern_array, 1);

    /* Overlay pattern if needed: */
    if (options.pattern_file != null)
      options.pattern_array = readPattern(buffer_size_times2);

    /* Copy the pattern to JNI: */
    Native.store_pattern(options.pattern_array);
  }


  /**
   * Compression pattern buffer must be at least 1MB.
   * (1MB minimum required in JNI for dedup and compression pattern data copies)
   *
   * By now allowing some writes to be done from the data pattern buffer we have
   * a problem: if we have a 16k write from the END of the pattern buffer we
   * have to create a separate buffer with the last 8k and first 8k of the
   * pattern stuck together.
   * That's too much effort, and also requires extra overhead since we are
   * trying to save cycles.
   *
   * Solution: allocate a pattern buffer that is TWICE as long and put the
   * pattern in there, but, as far as 'which offset in the pattern buffer do we
   * write from', don't tell the code about that.
   * This means that the write operation has one piece of contiguous memory to
   * write from without the OS blowing up.
   *
   * We also now will start with a minimum requirement of 4 MB, allowing for
   * possible future different compression algorithms that handle 4MB+ history.
   * This then results (see above) in a 2*4=8m pattern buffer.
   *
   * However, size returned HERE does NOT include the doubling!
   *
   *
   * Note: having too large of a pattern buffer size may have impact on
   * performance related to "how many bytes of this are messing up contents of
   * CPU L1, L2 cache".
   * With RDMA this though should not be a problem, especially since WE are no
   * longer copying. (This assumes that a CPU is not involved with rdma?)
   */
  private static int calculatePatternBufferSize(int max_xfer)
  {
    int MB      = 1 * 1024 * 1024;
    int minimum = MB * Validate.getPatternMB();
    if (max_xfer <= minimum)
      return minimum;

    /* Return the maximum xfersize used, rounded upwards to 1mb: */
    return (max_xfer + MB - 1) / MB * MB;
  }
  public static int getBufferSize()
  {
    return buffer_size;
  }


  public static void main(String[] args)
  {
    String arg = args[0];
    int bytes;
    if (arg.endsWith("k"))
      bytes = Integer.parseInt(arg.substring(0, arg.length() - 1)) * 1024;
    else if (arg.endsWith("m"))
      bytes = Integer.parseInt(arg.substring(0, arg.length() - 1)) * 1024 * 1024;
    else
      bytes = Integer.parseInt(arg);
    common.ptod("calculatePatternBufferSize(kb): %,12d %,12d", bytes, calculatePatternBufferSize(bytes));
  }


  /**
   * Create a data pattern that results in a proper compression rate.
   *
   * input:  - the compression: 100 bytes in, 'n' bytes out
   */
  private static void createCompressionPattern(int[]  temp_buffer,
                                               double comp_ratio)
  {
    long seed = Validate.getCompSeed();
    Random compression_random = new Random(seed);
    double set_zeros_limit = 0;

    /* Determine which randomizer limit to use: */
    if (!common.get_debug(common.DEBUG_COMPRESSION))
    {
      if (comp_ratio > 25)
      {
        /* Prevent people from asking for compratio=100, thinking it is compression=100? */
        common.failure("'compratio=25' is the largest value Vdbench supports. "+
                       "Are you sure your request is valid, asking for compratio=%.2f? ", comp_ratio);
        comp_ratio = 25;
      }
    }

    /* Look in the table for the zeros we're looking for: */
    set_zeros_limit = 0;
    for (int i = 0; i < randomizer_value.length; i+=2)
    {
      set_zeros_limit = randomizer_value[i+1];
      if (randomizer_value[i] >= comp_ratio)
        break;
    }

    //common.ptod("createCompressionPattern() seed: %d comp_ratio: %6.2f limit: %6.2f", seed, comp_ratio, set_zeros_limit);

    /* Fill buffer with random numbers: */
    for (int i = 0; i < temp_buffer.length; i++)
      temp_buffer[i] = (int) (compression_random.nextDouble() * Integer.MAX_VALUE);

    if (common.get_debug(common.DEBUG_COMPRESSION))
    {
      common.ptod("comp_ratio: " + comp_ratio);
      set_zeros_limit = comp_ratio;
    }

    /* Now replace 'comp'% of buffer with zeros: */
    int zeros = 0;
    for (int i = 0; i < temp_buffer.length; i++)
    {
      if (compression_random.nextDouble() * 100 < set_zeros_limit / 100.)
      {
        zeros++;
        temp_buffer[i] = 0;
      }
    }
    //common.ptod("zeros: " + zeros + " " + comp_ratio / 100);

    if (print_once++ == 999999999)
    {
      for (int i = 0; i < temp_buffer.length;)
      {
        String line = String.format("0x%08x ", i*4);
        for (int j = 0; j < 8; j++, i++)
          line += String.format("%08x ", temp_buffer[i]);
        common.ptod(line);
      }
    }
  }

  /**
   * Read pattern from pattern file and translate it to int array
   */
  private static int[] readPattern(int xfersize)
  {
    /* If the file does not exist, that's easy: */
    /* (This was checked during parsing, but now we may be on a different host) */
    File fptr = new File(options.pattern_file);
    if (!fptr.exists())
      common.failure("readPattern() pattern file does not exist: " +
                     new File(options.pattern_file).getAbsolutePath());

    int pattern[] = new int[(int) xfersize / 4];
    int buffer[]  = new int[(int) xfersize];
    int byteindex = 0;

    try
    {
      FileInputStream file_in = new FileInputStream(fptr);
      DataInputStream data_in = new DataInputStream(file_in);

      while (byteindex < xfersize)
      {
        try
        {
          int abyte = data_in.readUnsignedByte();
          buffer[byteindex++] =  abyte;
        }
        catch (IOException e)
        {
          break;
        }
      }

      if (byteindex == 0)
        common.failure("Pattern file empty: " + fptr.getAbsolutePath());
      data_in.close();

      /* Must be a 4 byte boundary; truncate (likely cr/lf): */
      if (byteindex % 4 != 0)
      {
        common.ptod("Data pattern length for file '" + fptr.getAbsolutePath() +
                    "' must be a multiple of 4. Input length of " +
                    byteindex + " truncated. Possible cr/lf? ");
        byteindex -= byteindex % 4;
      }
      if (byteindex == 0)
      {
        common.failure("Data pattern length for file '" + fptr.getAbsolutePath() +
                       "' must be a minimum of 4 bytes ");
      }

      /* We now have our bytes; translate them to ints: */
      for (int i = 0; i < xfersize ; i += 4)
      {
        long word = 0;
        word += buffer[ (i+0) % byteindex ] << 24;
        word += buffer[ (i+1) % byteindex ] << 16;
        word += buffer[ (i+2) % byteindex ] <<  8;
        word += buffer[ (i+3) % byteindex ];
        pattern[ i / 4 ] = (int) word;
      }

      common.ptod("Successfully loaded data pattern from " + fptr.getAbsolutePath());
    }
    catch (Exception e)
    {
      common.failure(e);
    }

    return pattern;
  }


  /**
   * Copy the data pattern to the data buffer.
   */
  public static void copyPatternToBuffer(long buffer, int bufsize)
  {
    Native.arrayToBuffer(getPattern(), buffer, bufsize);
  }


  /**
   * Copy default data pattern to the data buffer.
   */
  public static void storeStartingSdPattern(long buffer, int size)
  {
    if (Validate.isCompression() || options.pattern_file != null)
      Native.arrayToBuffer(getPattern(), buffer, size);

    else
      Patterns.copyPatternToBuffer(buffer, size);
  }


  /**
   * Copy default data pattern to the data buffer.
   */
  public static void storeStartingFsdPattern(long buffer, int size)
  {
    if (Validate.isCompression() || options.pattern_file != null)
    {
      Native.arrayToBuffer(getPattern(), buffer, size);
    }

    else
      Patterns.copyPatternToBuffer(buffer, size);
  }


  public static void parsePattern(String[] parms)
  {
    if (parms.length > 1)
      common.failure("Only ONE pattern= parameter value may be coded. ");

    String parm = parms[0];

    if (options.pattern_coded)
      common.failure("'pattern=' may be used only once.");

    options.pattern_coded = true;

    options.pattern_file = parm;
  }


  /**
   * Check to make sure that we properly use the data pattern options.
   *
   * A trick is used here to allow the specification of a data pattern file
   * while still using Data Validation.
   * Realize though that no matter what the pattern is, Vdbench will still take
   * the first 32 bytes of each 512byte sector for itself.
   */
  public static void checkPattern()
  {
    if (Dedup.isDedup() && options.pattern_coded)
      common.failure("parsePatterns(): dedupratio= and pattern= are mutually exclusive");

    if (!common.get_debug(common.DV_ALLOW_PATTERN))
    {
      if (Validate.isValidate() && options.pattern_coded)
        common.failure("parsePatterns(): validate=yes and pattern= are mutually exclusive");

      //if (Patterns.getDefaultCompressionRatio() != -1 && options.pattern_coded)
      if (Validate.isCompressionRequested() && options.pattern_coded)
        common.failure("parsePatterns(): compratio= and pattern= are mutually exclusive");
    }

    /* Allow use of patterns=file */
    else
    {
      common.ptod("User requested the use of patterns=/file/name");
      common.ptod("This by default is not allowed when Data Validation is active.");
      common.ptod("This check is now bypassed.");

      /* An override of compratio is required to prevent JNI code from using LFSR. */
      /* The ratio however is NOT used.                                            */
      Patterns.setDefaultCompressionRatio(2);
    }
  }


  private static double[] set7000Numbers()
  {
    double[] numbers =
    {
      1.00  , 0000,
      1.10  , 3500,
      1.15  , 3600,
      1.16  , 3700,
      1.17  , 3800,
      1.19  , 3900,
      1.20  , 4000,
      1.21  , 4100,
      1.23  , 4200,
      1.24  , 4300,
      1.26  , 4400,
      1.27  , 4500,
      1.29  , 4600,
      1.31  , 4700,
      1.32  , 4800,
      1.34  , 4900,
      1.36  , 5000,
      1.38  , 5100,
      1.40  , 5200,
      1.42  , 5300,
      1.45  , 5400,
      1.47  , 5500,
      1.49  , 5600,
      1.52  , 5700,
      1.55  , 5800,
      1.57  , 5900,
      1.60  , 6000,
      1.64  , 6100,
      1.67  , 6200,
      1.70  , 6300,
      1.74  , 6400,
      1.78  , 6500,
      1.82  , 6600,
      1.87  , 6700,
      1.92  , 6800,
      1.97  , 6900,
      2.02  , 7000,
      2.08  , 7100,
      2.13  , 7200,
      2.20  , 7300,
      2.27  , 7400,
      2.34  , 7500,
      2.43  , 7600,
      2.52  , 7700,
      2.62  , 7800,
      2.72  , 7900,
      2.84  , 8000,
      2.96  , 8100,
      3.10  , 8200,
      3.25  , 8300,
      3.42  , 8400,
      3.62  , 8500,
      3.83  , 8600,
      4.06  , 8700,
      4.33  , 8800,
      4.66  , 8900,
      5.04  , 9000,
      5.08  , 9010,
      5.12  , 9020,
      5.18  , 9030,
      5.21  , 9040,
      5.26  , 9050,
      5.29  , 9060,
      5.35  , 9070,
      5.40  , 9080,
      5.45  , 9090,
      5.49  , 9100,
      5.55  , 9110,
      5.60  , 9120,
      5.67  , 9130,
      5.71  , 9140,
      5.74  , 9150,
      5.84  , 9160,
      5.87  , 9170,
      5.90  , 9180,
      5.97  , 9190,
      6.03  , 9200,
      6.12  , 9210,
      6.19  , 9220,
      6.19  , 9230,
      6.30  , 9240,
      6.34  , 9250,
      6.38  , 9260,
      6.50  , 9270,
      6.55  , 9280,
      6.63  , 9290,
      6.67  , 9300,
      6.76  , 9310,
      6.81  , 9320,
      6.90  , 9330,
      6.99  , 9340,
      7.04  , 9350,
      7.16  , 9360,
      7.24  , 9370,
      7.34  , 9380,
      7.40  , 9390,
      7.56  , 9400,
      7.59  , 9410,
      7.68  , 9420,
      7.82  , 9430,
      7.85  , 9440,
      7.97  , 9450,
      8.07  , 9460,
      8.23  , 9470,
      8.33  , 9480,
      8.40  , 9490,
      8.61  , 9500,
      8.65  , 9510,
      8.80  , 9520,
      8.91  , 9530,
      9.07  , 9540,
      9.23  , 9550,
      9.40  , 9560,
      9.49  , 9570,
      9.62  , 9580,
      9.76  , 9590,
      9.95  , 9600,
      10.15 , 9610,
      10.25 , 9620,
      10.46 , 9630,
      10.62 , 9640,
      10.85 , 9650,
      11.02 , 9660,
      11.21 , 9670,
      11.39 , 9680,
      11.65 , 9690,
      11.79 , 9700,
      12.21 , 9710,
      12.23 , 9720,
      12.44 , 9730,
      12.82 , 9740,
      12.98 , 9750,
      13.50 , 9760,
      13.50 , 9770,
      13.95 , 9780,
      14.25 , 9790,
      14.55 , 9800,
      14.98 , 9810,
      15.20 , 9820,
      15.78 , 9830,
      16.03 , 9840,
      16.55 , 9850,
      16.97 , 9860,
      17.25 , 9870,
      18.00 , 9880,
      18.17 , 9890,
      19.37 , 9900,
      19.56 , 9910,
      20.54 , 9920,
      21.18 , 9930,
      21.62 , 9940,
      23.09 , 9950,
      23.09 , 9960,
      24.76 , 9970,
      25.38 , 9980,
      25.69 , 9990
    };
    return numbers;
  }
}

