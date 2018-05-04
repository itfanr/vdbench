

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
#include <time.h>
#include <jni.h>
#include "vdbjni.h"


extern void snap(JNIEnv *env, char *text, void* start, int length)
{

  int i, j, k;
  char worktext[20+1];
  char *dump;
  char line[100];
  char newline[100];
  char oldline[100];
  char work[100];
  char txt[100];
  char tmp[100];

  memset(worktext, 0, sizeof(worktext));
  memcpy(worktext, text, 20);
  dump = (char*) start;
  memset(oldline, 0, sizeof(oldline));
  oldline[0] = 0;


  for (i = 0; i < length; )
  {
    newline[0] = 0;
    txt[0] = 0;
    sprintf(work, "%-16s %08x (+%04x): ", worktext, dump, i);
    strcat(newline, work);
    worktext[0] = 0;

    for (j = 0; j < 4; j++)
    {
      for (k = 0; (k < 4) && (i < length); k++, i++, dump++)
      {
        sprintf(work, "%02X", (unsigned char) *dump);
        strcat(newline, work);

        if ( *dump >= 0x21 && *dump <= 0x7a)
        {
          sprintf(work, "%c", *dump);
          strcat(txt, work);
        }
        else
          strcat(txt, ".");

      }
      sprintf(work, " ");
      strcat(newline, work);
    }

    if (strcmp(&oldline[36], &newline[36]) != 0)
    {
      sprintf(tmp, "%-68s%s ", newline, txt);
      PTOD(tmp);
    }

    strcpy(oldline,newline);
  }
  PTOD("");
}


