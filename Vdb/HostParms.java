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

import java.util.*;

/**
 * This class handles HOST parameters
 */
public class HostParms
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";

  private static Host dflt_host  = new Host();


  /**
   * Read Host information.
   */
  static String readParms(String first)
  {
    Host current_host = null;
    String str = first;
    Vdb_scan prm;
    int warnings = 0;

    if (str == null)
      common.failure("Early EOF on input parameters");

    while (true)
    {
      prm = Vdb_scan.parms_split(str);

      if (prm.keyword.equals("rg")  ||
          prm.keyword.equals("sd")  ||
          prm.keyword.equals("fsd") ||
          prm.keyword.equals("wd")  ||
          prm.keyword.equals("fwd") ||
          prm.keyword.equals("rd")  )
        break;

      /* Host keyword: */
      if (prm.keyword.equals("host") && warnings++ == 0)
      {
        common.ptod("");
        common.ptod("Specifying 'host=(xxx.yyy.com,host_x) has been deprecated.");
        common.ptod("Please specify 'hd=host_x,system=xxx.yyy.com' instead.");
        common.ptod("Vdbench will honor this parameter until the next release.");
        common.ptod("");


      }

      if (prm.keyword.equals("hd") || prm.keyword.equals("host"))
      {
        if (prm.keyword.equals("host") || prm.getAlphaCount() > 1)
        {
          if (warnings++ == 0)
          {
            common.ptod("Specifying 'hd=(xxx.yyy.com,host_x) or");
            common.ptod("Specifying 'host=(xxx.yyy.com,host_x) has been deprecated.");
            common.ptod("Please specify 'hd=host_x,system=xxx.yyy.com' instead.");
            common.ptod("Vdbench will honor this parameter until the next release.");
          }
        }


        /* Either set the default or create a new one: */
        if (prm.alphas[0].equals("default"))
          current_host = dflt_host;

        else
        {
          current_host = (Host) dflt_host.clone();

          /* A host by default uses the host label as host name/ip: */
          current_host.host_label = prm.alphas[0];
          current_host.host_ip    = prm.alphas[0];

          /* Use old style label parameter: */
          if (prm.getAlphaCount() > 1)
            current_host.host_label = prm.alphas[1];

          Host.addHost(current_host);
        }
      }

      else if ("system".startsWith(prm.keyword))
      {
        current_host.host_ip = prm.alphas[0];
        if (!Host.doesHostExist(current_host.host_ip))
          common.failure("Host '" + current_host.host_ip + "' does not exist");
      }

      else if ("vdbench".startsWith(prm.keyword))
        current_host.host_vdbench = prm.alphas[0];

      else if ("shell".startsWith(prm.keyword))
        current_host.host_shell = prm.alphas[0];

      else if ("user".startsWith(prm.keyword))
        current_host.host_user = prm.alphas[0];

      else if ("jvms".startsWith(prm.keyword))
      {
        current_host.setJvmCount((int) prm.numerics[0]);
        current_host.jvms_in_parm = true;
      }

      else if ("clients".startsWith(prm.keyword))
        current_host.client_count = (int) prm.numerics[0];

      else if ("mount".startsWith(prm.keyword))
      {
        if (current_host == dflt_host)
          common.failure("'mount=' parameter not allowed for hd=default");
        current_host.host_mount = new Mount(prm.alphas);
      }

      else
        common.failure("HostParms.ReadParms(): invalid keyword: " + prm.keyword);

      str = Vdb_scan.parms_get();
      if (str == null)
        common.failure("Early EOF on input parameters");
    }

    /* Make sure we have at least one host defined (localhost): */
    if (Host.getDefinedHosts().size() == 0)
      Host.addHost((Host) dflt_host.clone());


    /* Repeat hosts for client_count: */
    Vector hosts = Host.getDefinedHosts();
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);
      if (host.client_count > 0)
      {
        /*  First create the extras: */
        for (int j = 1; j < host.client_count; j++)
        {
          Host host2 = (Host) host.clone();
          host2.host_label  += "_client" + j;
          host2.client_count = 0;
          Host.addHost(host2);
          common.plog("Added client: " + host2.host_label);
        }

        /* Then modify the original: */
        host.client_count = 0;
        host.host_label += "_client0";
        common.plog("Added client: " + host.host_label);
      }
    }

    /* Make sure we end with a separator for each vdbench directory: */
    hosts = Host.getDefinedHosts();
    for (int i = 0; i < hosts.size(); i++)
    {
      Host host = (Host) hosts.elementAt(i);

      if (!host.host_vdbench.endsWith("/") &&
          !host.host_vdbench.endsWith("\\"))
      {
        if (host.host_vdbench.indexOf("/") != -1)
          host.host_vdbench += "/";
        else if (host.host_vdbench.indexOf("\\") != -1)
          host.host_vdbench += "\\";
        else
          common.failure("'vdbench=' directory name must end with a file separator (\\ or /): " + host.host_vdbench);
      }
    }

    return str;
  }

}


