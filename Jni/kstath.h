

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


#include <kstat.h>         /* kstat_t       */
#include <jni.h>



struct Cpu
{
  jlong cpu_count;
  jlong cpu_total;
  jlong cpu_idle;
  jlong cpu_user;
  jlong cpu_kernel;
  jlong cpu_wait;
  jlong usecs_per_tick;
};


extern kstat_ctl_t *global_kstat_kc;  /* Pointer for kstat_read()            */

extern kstat_t* get_kstat_t(JNIEnv *env, const char *instance);

extern void get_cpu_times(struct Cpu *cpu);



