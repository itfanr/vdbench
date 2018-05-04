
/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

#include "vdbjni.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "kstath.h"
#include <sys/sysinfo.h>


kstat_ctl_t *global_kstat_kc = 0;    /* libkstat cookie */
static  int ncpus;
static  int usecs_per_tick;
static  kstat_t **kpu_stat_list = NULL;
static  cpu_stat_t  cpu_stat;


static char c[] =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";


#define fail(a,b)        \
{                        \
  printf("%s\n", #b);    \
  exit(a);               \
}



static void safe_zalloc(void **ptr, int size, int free_first)
{
  if (free_first && *ptr != NULL)
    free(*ptr);
  if ((*ptr = (void *)malloc(size)) == NULL)
    fail(1, "malloc failed");
  (void) memset(*ptr, 0, size);
}

static void cpu_stat_close(void)
{
  kstat_close(global_kstat_kc);
  global_kstat_kc = 0;
}

/*
 * Get list of cpu_stat KIDs for subsequent cpu_stat_load operations.
 */

static void cpu_stat_init(void)
{
  kstat_t *ksp;
  int hz;

  /* First time: open kstat: */
  if (global_kstat_kc == 0)
  {
    if ((global_kstat_kc = kstat_open()) == NULL)
      fail(1, "kstat_open(): can't open /dev/kstat");
    hz = sysconf(_SC_CLK_TCK);
    usecs_per_tick = 1000000 / hz;
  }

  /* Get the total number of cpus: */
  ncpus = 0;
  for (ksp = global_kstat_kc->kc_chain; ksp; ksp = ksp->ks_next)
    if (strncmp(ksp->ks_name, "cpu_stat", 8) == 0)
      ncpus++;

  /* Allocate a table for all the cpu stuff: */
  safe_zalloc((void **)&kpu_stat_list, ncpus * sizeof (kstat_t *), 1);

  /* Get all the kstat pointers for the cpus: */
  ncpus = 0;
  for (ksp = global_kstat_kc->kc_chain; ksp; ksp = ksp->ks_next)
    if (strncmp(ksp->ks_name, "cpu_stat", 8) == 0 &&
        kstat_read(global_kstat_kc, ksp, NULL) != -1)
      kpu_stat_list[ncpus++] = ksp;

  if (ncpus == 0)
    fail(1, "can't find any cpu statistics");

  (void) memset(&cpu_stat, 0, sizeof (cpu_stat_t));
}



static int cpu_stat_load(void)
{
  int i, j;
  cpu_stat_t cs;
  ulong *np, *tp;

  (void) memset(&cpu_stat, 0, sizeof (cpu_stat_t));

  /* Sum across all cpus */
  for (i = 0; i < ncpus; i++)
  {
    /* Read one cpu's data: */
    if (kstat_read(global_kstat_kc, kpu_stat_list[i], (void *)&cs) == -1)
      return(1);

    /* Set address of array of int to loop through and add statistics: */
    np = (ulong *)&cpu_stat.cpu_sysinfo;
    tp = (ulong *)&cs.cpu_sysinfo;


    /* Accumulate this array of statistics: */
    for (j = 0; j < CPU_STATES * sizeof (ulong_t); j += sizeof (ulong_t))
    {
      *np++ += *tp++;
    }

  }

  return 0;
}

extern void get_cpu_times(struct Cpu *cpu)
{
  jlong   tot_cpu;
  int    i;
  int    interval = 1;
  static int first =  1;

  if (first)
  {
    first = 0;
    cpu_stat_init();
    cpu_stat_load();
  }

  while ( cpu_stat_load() )
  {
    (void) printf("<<State change>>\n");
    (void) kstat_chain_update(global_kstat_kc);
    cpu_stat_init();
  }

  /* Calculate the total amount of cpu utilization: */
  tot_cpu = 0;
  for (i = 0; i < CPU_STATES; i++)
    tot_cpu += cpu_stat.cpu_sysinfo.cpu[i];


#ifdef xxx

  printf(" %12d %12d %12d %12d %12lld",
         cpu_stat.cpu_sysinfo.cpu[CPU_USER] ,
         cpu_stat.cpu_sysinfo.cpu[CPU_KERNEL] ,
         cpu_stat.cpu_sysinfo.cpu[CPU_WAIT] ,
         cpu_stat.cpu_sysinfo.cpu[CPU_IDLE],
         tot_cpu );
  printf("\n");

#endif

  cpu->cpu_count      = ncpus;
  cpu->cpu_total      = tot_cpu;
  cpu->cpu_idle       = cpu_stat.cpu_sysinfo.cpu[CPU_IDLE];
  cpu->cpu_user       = cpu_stat.cpu_sysinfo.cpu[CPU_USER];
  cpu->cpu_kernel     = cpu_stat.cpu_sysinfo.cpu[CPU_KERNEL];
  cpu->cpu_wait       = cpu_stat.cpu_sysinfo.cpu[CPU_WAIT];
  cpu->usecs_per_tick = usecs_per_tick;
}


