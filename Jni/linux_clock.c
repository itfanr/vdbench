
/*
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.
 */

/*
 * Author: Henk Vandenbergh.
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>

static char c[] =
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved.";


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

