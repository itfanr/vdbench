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

import java.util.Date;
import java.io.Serializable;
import Utils.Bin;

/**
 * Binary record for Swat
 */
abstract class Bin_record extends VdbCount implements Serializable
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private byte record_type    = -1;
  private byte record_version = -1;

  //private static Instances counters = new Instances("Bin_record", 0);

  abstract public void export(Bin bin);
  abstract public void emport(Bin bin);

  public Bin_record(byte type, byte version)
  {
    record_type    = type;
    record_version = version;
    //counters.add(this);
  }

  public byte getType()
  {
    return record_type;
  }

  public byte getversion()
  {
    return record_version;
  }
}
