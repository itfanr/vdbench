package Vdb;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;



/**
 * Class to keep track of GC counts and time.
 */
public class GcTracker
{

  private long   tod      = 0;
  private long   elapsed  = 0;
  private long   gc_count = 0;
  private long   ccount   = 0;
  private long   gc_time  = 0;
  private long   gc_beans = 0;


  private static boolean   report_now   = false;
  private static GcTracker prev_tracker = new GcTracker();

  /**
   * Create a new instance containing all CURRENT information.
   */
  public GcTracker()
  {
    tod = System.currentTimeMillis();
    for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans())
    {
      gc_beans++;
      long new_count = gc.getCollectionCount();

      if (new_count >= 0)
        gc_count += new_count;

      long new_time = gc.getCollectionTime();

      if (new_time >= 0)
        gc_time += new_time;
    }

    ccount = gc_count;
  }

  public static void gc()
  {
    common.ptod("Requesting full garbage collection");
    reportAlways();
    System.gc();
    reportAlways();
  }

  /**
   * Get latest GC statistics, calculate delta, and report delta.
   *
   * Conditions for reporting:
   * - get_debug() is set
   * or
   * - more than 100ms GC duration
   */
  public static void reportAlways()
  {
    report_now = true;
    report();
  }
  public static void report()
  {

    GcTracker new_tracker = new GcTracker();

    long elapsed  = new_tracker.tod       - prev_tracker.tod;
    long gc_count = new_tracker.gc_count  - prev_tracker.gc_count;
    long gc_time  = new_tracker.gc_time   - prev_tracker.gc_time;
    long ccount   = new_tracker.ccount;
    long gc_beans = new_tracker.gc_beans;

    double free    = (double) Runtime.getRuntime().freeMemory()  / 1048576.0;
    double current = (double) Runtime.getRuntime().totalMemory() / 1048576.0;
    double max     = (double) Runtime.getRuntime().maxMemory()   / 1048576.0;
    double used    = current - free;

    /* prevent divide by zero: */
    if (elapsed == 0)
      elapsed = 1;

    prev_tracker   = new_tracker;

    if (!report_now)
    {
      if (common.get_debug(common.GCTRACKER))
      {
        report_now = true;
      }

      else if (gc_count > 0 && gc_time > (100 * 1000))
      {
        report_now = true;
      }
    }

    /* Only report if any GC has been done: */
    if (!report_now)
      return;

    double pct       = gc_time * 100. / elapsed;
    double msecs_sec = gc_time / elapsed / 1000;
    String txt = String.format("GcTracker: cum: %3d intv: %2d ms: %,7d mss: "+
                               "%5.2f%% " +
                               "Heap_MB max: %5.0f curr: %5.0f used: %5.0f free: %5.0f ",
                               ccount,                 // cumulative count
                               gc_count,               // GCs in last interval
                               gc_time,                // Total ms in last interval
                               msecs_sec,              // ms per second
                               //gc_beans,
                               max, current, used, free);
    common.plog(txt);

    report_now = false;
  }


  public static void main(String[] args) throws Exception
  {
    for (int i = 0; i < 5; i++)
    {
      common.sleep_some(1000);
      System.gc();
      GcTracker.report();
    }
  }

}


