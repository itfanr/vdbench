package Vdb;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import Utils.*;


/**
 * This class contains all stuff needed to accumulate FWD statistics.
 * There is one instance for each operation: read, write, etc.
 */
class FwdCounter implements java.io.Serializable
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private FwdStats  fstats;   /* Needed to find elapsed time */
  public  long      operations;
  public  long      response;
  private Histogram histogram =   new Histogram("default");



  public FwdCounter(FwdStats st)
  {
    fstats = st;
  }

  public Histogram getHistogram()
  {
    return histogram;
  }

  public void addResp(long resp)
  {
    operations ++;

    response += resp;
    histogram.addToBucket(resp);
  }

  public void accum(FwdCounter old)
  {
    operations += old.operations;
    response   += old.response;
    histogram.accumBuckets(old.histogram);
  }

  public void delta(FwdCounter nw, FwdCounter old)
  {
    operations = nw.operations - old.operations;
    response   = nw.response   - old.response;
    histogram.deltaBuckets(nw.histogram, old.histogram);
  }

  public void copy(FwdCounter old)
  {
    operations = old.operations;
    response   = old.response;
    histogram  = (Histogram) old.histogram.clone();
  }

  public double rate()
  {
    //common.ptod("stats.elapsed: " + operations + " " + fstats.elapsed);
    return operations * 1000000. / fstats.getElapsed();
  }
  public double resp()
  {
    if (operations == 0)
      return 0;
    return(double) response / operations / 1000.;
  }
}
