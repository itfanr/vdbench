package User;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import Vdb.common;


/**
 * Allow user level synchronization.
 * A request is sent to this code by each UserClass instance. As soon as every
 * thread is waiting all threads will be released
 */
public class UserSync
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private static HashMap <String, Vector> sema_list = new HashMap(16);

  public static boolean waitForSync(int waiters, Object obj)
  {
    Semaphore new_sema = new Semaphore(0);
    boolean this_is_the_last = false;

    /* Only one update at the time: */
    synchronized (sema_list)
    {
      String classname = obj.getClass().getName();

      /* If we don't know this class, add it to the map: */
      Vector list      = sema_list.get(classname);
      if (list == null)
      {
        list = new Vector(waiters);
        sema_list.put(classname, list);
      }

      /* Add a new semaphore for this class: */
      list.add(new_sema);

      /* If we have reached our required amount of waiters, let them all go: */
      if (list.size() == waiters)
      {
        this_is_the_last = true;
        for (int i = 0; i < waiters; i++)
        {
          Semaphore sema = (Semaphore) list.elementAt(i);
          sema.release();
        }
        list.removeAllElements();
      }
    }

    /* Wait. If I was the last one, this semaphore has just been released: */
    try
    {
      new_sema.acquire();
    }
    catch (InterruptedException e)
    {
    }

    return this_is_the_last;
  }
}

