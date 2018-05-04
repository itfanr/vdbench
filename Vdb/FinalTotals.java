package Vdb;

/*
 * Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

import java.io.PrintWriter;
import java.util.Date;

import Utils.Format;

/**
 * Accumulate totals to be reported at the end of execution.
 * These totals cover all combined runs.
 * The timing of the reporting was changed after it was found out that the
 * addShutdownHook() Java function on Solaris is unreliable.
 * Data now reported every interval when using the 'reportruntotals' option.
 */
public class FinalTotals
{
  private final static String c =
  "Copyright (c) 2000, 2015, Oracle and/or its affiliates. All rights reserved.";

  private static SdStats total_sd    = new SdStats();
  private static Kstat_data total_ks = new Kstat_data();

  public static void add(SdStats stats)
  {
    total_sd.stats_accum(stats, true);
  }
  public static void add(Kstat_data stats)
  {
    total_ks.kstat_accum(stats, true);
  }



  /**
   * Special request from Paul Carr, Paul Fischer and Wayne ErtzBischoff: report
   * totals for very large runs.
   */
  public static void printTotals(int interval)
  {
    if (total_sd.reads + total_sd.writes == 0)
      return;

    /* This is an undocumented option, and is NOT the normal 'totals.html' */
    PrintWriter pw = Report.createHmtlFile("totals_optional.html");

    double GB = 1024 * 1024 * 1024;
    double gigabytesr = total_sd.r_bytes / GB;
    double gigabytesw = total_sd.w_bytes / GB;
    double gigabytes  = gigabytesr + gigabytesw;
    double errors_gb  = (ErrorLog.getErrorCount() == 0) ? 0 : (ErrorLog.getErrorCount() / gigabytes);

    String tod = common.tod();
    pw.println("");
    pw.println("This report is created at the end of each successfully completed reporting interval.");
    pw.println("Last reported interval is interval #" + interval + " on " + new Date());
    pw.println("");

    pw.println("");
    pw.println("Overall execution totals logical i/o: ");
    pw.println(Format.f("Total iops:             %12.2f", total_sd.rate()));
    pw.println(Format.f("Total reads+writes:     %12d", (total_sd.reads + total_sd.writes)));
    pw.println(Format.f("Total gigabytes:        %12.2f", gigabytes));
    pw.println(Format.f("Total gigabytes read:   %12.2f", gigabytesr));
    pw.println(Format.f("Total gigabytes write:  %12.2f", gigabytesw));
    pw.println(Format.f("Total reads:            %12d", total_sd.reads));
    pw.println(Format.f("Total writes:           %12d", total_sd.writes));
    pw.println(Format.f("Total readpct:          %12.2f", total_sd.readpct()));
    pw.println(Format.f("Total I/O or DV errors: %12d", ErrorLog.getErrorCount()) +
                Format.f(" (%.12f errors per GB)", errors_gb));
    pw.println("");

    if (!common.onSolaris())
      return;

    gigabytesr = total_ks.nread / GB;
    gigabytesw = total_ks.nwritten / GB;
    gigabytes  = gigabytesr + gigabytesw;
    errors_gb  = (ErrorLog.getErrorCount() == 0) ? 0 : (ErrorLog.getErrorCount() / gigabytes);

    pw.println("");
    pw.println("Overall execution totals physical i/o: ");
    pw.println(Format.f("Total iops:             %8.2f", total_ks.kstat_rate()));
    pw.println(Format.f("Total reads+writes:     %8d", (total_ks.reads + total_ks.writes)));
    pw.println(Format.f("Total gigabytes:        %8.2f", gigabytes));
    pw.println(Format.f("Total gigabytes read    %8.2f", gigabytesr));
    pw.println(Format.f("Total gigabytes write   %8.2f", gigabytesw));
    pw.println(Format.f("Total reads:            %8d", total_ks.reads));
    pw.println(Format.f("Total writes:           %8d", total_ks.writes));
    pw.println(Format.f("Total readpct:          %8.2f", total_ks.kstat_readpct()));
    pw.println(Format.f("Total I/O or DV errors: %8d", ErrorLog.getErrorCount()) +
                Format.f(" (%.8f errors per GB)", errors_gb));
    pw.println("");

    pw.close();
  }
}

