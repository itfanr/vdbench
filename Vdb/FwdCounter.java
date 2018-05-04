package Vdb;

/*
 * Copyright 2010 Sun Microsystems, Inc. All rights reserved.
 *
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * The contents of this file are subject to the terms of the Common
 * Development and Distribution License("CDDL") (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the License at http://www.sun.com/cddl/cddl.html
 * or ../vdbench/license.txt. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * When distributing the software, include this License Header Notice
 * in each file and include the License file at ../vdbench/licensev1.0.txt.
 *
 * If applicable, add the following below the License Header, with the
 * fields enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */


/*
 * Author: Henk Vandenbergh.
 */


import Utils.*;


/**
 * This class contains all stuff needed to accumulate FWD statistics.
 * There is one instance for each operation: read, write, etc.
 */
class FwdCounter extends VdbObject implements java.io.Serializable
{
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
    response   += resp;
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
