

/*
 * Copyright (c) 2010 Sun Microsystems, Inc. All rights reserved.
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


#include <stdio.h>
#include <string.h>
#include <unistd.h>


/**
 * All that we do here is return the clock tick count which is
 * needed to calculate /proc/diskstat values.
 */
int main(int argc, char **argv)
{
  int ticks;

  if ((ticks = sysconf(_SC_CLK_TCK)) == -1)
    printf("Error retrieve clock tick count");
  else
    printf("ticks: %d\n", ticks);

  return 0;
}

