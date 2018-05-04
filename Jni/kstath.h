
/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

#include <kstat.h>         /* kstat_t       */

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



