package User;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Vector;

import Vdb.*;

/**
 * This class takes care of the creation of new UserClass instances.
 */
public class ControlUsers
{
  private final static String c = 
  "Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved."; 

  private static boolean any_users = false;
  private static HashMap <String, UserInfo> userinfo_map = new HashMap(16);

  private static HashMap missing_methods = new HashMap(16);

  /**
   * Create instance for all RD_entry's and for all its WG_entry's.
   */
  public static UserClass createInstance(WG_entry wg)
  {
    String[] parms = wg.user_class_parms;

    /* Check for proper class name: */
    String cname = parms[0];
    checkClassname(cname);

    /* Strip the class name away from the parameters: */
    String[] stripped = new String[parms.length - 1];
    System.arraycopy(parms, 1, stripped, 0, parms.length - 1);

    /* Create the UserClass instance and call its parser: */
    UserInfo ui    = new UserInfo(wg.wd_name, cname, stripped);
    UserClass user = (UserClass) instantiate(ui);
    ui.setInstance(user);
    if (!user.parser(ui.getParms()))
      common.failure("ControlUsers.create(wg): parser failed for wd=%s,user=%s",
                     wg.wd_name, ui.getClassName());

    /* Remember which classes we have: */
    userinfo_map.put(cname, ui);

    any_users = true;

    return user;
  }

  public static void clearClasses()
  {
    userinfo_map = new HashMap(32);
  }

  private static void checkClassname(String cname)
  {
    try
    {
      Class.forName(cname);
    }
    catch (ClassNotFoundException e)
    {
      common.failure("checkClassname(): %s is not a valid class name", cname);
    }
  }


  private static Object instantiate(UserInfo ui)
  {
    try
    {
      ui.setInstance((UserClass) Class.forName(ui.getClassName()).newInstance());
      return ui.getInstance();
    }

    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }

  public static boolean anyUserClasses()
  {
    return any_users;
  }


  /**
   * Ask all user classes to pass me some information from a static method.
   * The information will be placed in a Vector and is passed to the master
   * JVM, where it is processed by receivedIntervalDataFromSlaves().
   *
   * The master JVM then will do with it whatever he needs and returns a Vector
   * with any data to all slave JVMs.
   */
  public static Vector getIntervalDataForMaster()
  {
    Vector returns = new Vector(16);
    String[] classes = userinfo_map.keySet().toArray(new String[0]);
    for (int i = 0; i < classes.length; i++)
    {
      UserInfo ui = userinfo_map.get(classes[i]);
      UserData[] udata = (UserData[]) callStaticMethod(ui, "getIntervalDataForMaster", null);
      if (udata != null)
        returns.add(udata);
    }
    return returns;
  }


  /**
  * Process data received from all slaves.
  * Pass all the data to static method UserClass.receivedIntervalDataFromSlaves().
  * At this time we do not allow any data to be passed back, and instead the user
  * can change the contents of the UserData if he wants something passed back to
  * the slaves.
  *
  */
  public static UserData[] receivedIntervalDataFromSlaves(CollectSlaveStats css)
  {
    /* First put all the data received into a UserData HashMap: */
    HashMap <UserData, UserData> user_map = new HashMap(64);
    for (int i = 0; i < css.getDataFromSlaves().length; i++)
    {
      SlaveStats sts = css.getDataFromSlaves()[i];
      if (sts.getUserData() != null)
      {
        Vector uvec = sts.getUserData();
        for (int j = 0; j < uvec.size(); j++)
        {
          UserData[] uds = (UserData[]) uvec.elementAt(j);
          for (int k = 0; k < uds.length; k++)
          {
            if (user_map.put(uds[k], uds[k]) != null)
              common.failure("receivedIntervalDataFromSlaves(): received duplicate data");
          }
        }
      }
    }

    /* First pick up performance information for all SDs.        */
    /* We'll have userData instances from multiple slaves, there */
    /* all instances will get the totals. :                      */
    UserData[] alldata = (UserData[]) user_map.values().toArray(new UserData[0]);
    for (int i = 0; i < alldata.length; i++)
    {
      UserData ud = alldata[i];
      SdStats stats = Report.getReport(ud.GetSdName()).getData().getIntervalSdStats();
      ud.setRate(stats.rate());
      ud.setRdPct(stats.readpct());
      ud.setResp(stats.respTime());
    }

    /* Now we pass all data (including stats) to every class: */
    String[] classes = userinfo_map.keySet().toArray(new String[0]);
    for (int i = 0; i < classes.length; i++)
    {
      UserInfo ui = userinfo_map.get(classes[i]);
      callStaticMethod(ui, "receivedIntervalDataFromSlaves", alldata);
    }

    /* This will be returned to all slaves and user classes: */
    return alldata;
  }


  /**
   * Receive the information that was sent by the master.
   */
  public static Vector receivedIntervalDataFromMaster(UserData[] udata)
  {
    Vector returns = new Vector(16);
    String[] classes = userinfo_map.keySet().toArray(new String[0]);
    for (int i = 0; i < classes.length; i++)
    {
      UserInfo ui = userinfo_map.get(classes[i]);
      Object ret = callStaticMethod(ui, "receivedIntervalDataFromMaster", udata);
      if (ret != null)
        returns.add(ret);
    }
    return returns;
  }


  public static void sendUserDataToSlaves(UserData[] udata)
  {
    for (int i = 0; i < SlaveList.getSlaveList().size(); i++)
    {
      Slave slave = (Slave) SlaveList.getSlaveList().elementAt(i);
      if (slave.getCurrentWork() != null)
      {
        SocketMessage sm = new SocketMessage(SocketMessage.USER_DATA_TO_SLAVES);
        sm.setData(udata);
        slave.getSocket().putMessage(sm);
      }
    }
  }



  /**
   * Call a static method within a User class.
   * Since these static methods are optional, report missing methods once and
   * continue.
   * To prevent this 'missing' message, just code a dummy method and return
   * null.
   */
  private static Object callStaticMethod(UserInfo ui, String method, Object parm)
  {
    try
    {
      /* First get the class: */
      Class wanted_class = Class.forName(ui.getClassName());

      /* Look for the method name: */
      Method[] methods = wanted_class.getMethods();
      for (int i = 0; i < methods.length; i++)
      {
        if (methods[i].getName().equals(method))
        {
          return methods[i].invoke(ui.getInstance(), new Object[] { parm});
        }
      }
    }

    catch (ClassNotFoundException e)
    {
      common.failure("callClass(): %s is not a valid class name", ui.getClassName());
    }
    catch (Exception e)
    {
      common.failure(e);
    }

    /* We did not find a proper method. Report it once: */
    //String label = ui.getClassName() + "." + method + "()";
    //if (missing_methods.get(label) == null)
    //  common.plog("callStaticMethod(): missing optional static method %s.%s().", ui.getClassName(), method);
    //missing_methods.put(label, label);

    return null;
  }

  public static void callDataMethods(String classname, String data)
  {
    UserInfo[] infos = (UserInfo[]) userinfo_map.values().toArray(new UserInfo[0]);

    for (int i = 0; i < infos.length; i++)
    {
      UserInfo ui = infos[i];
      try
      {
        common.ptod("ui.getInstance().getClass().getName(): " + ui.getInstance().getClass().getName());
        if (ui.getInstance().getClass().getName().equals(classname))
          ui.getInstance().passData(data);
      }
      catch (RuntimeException e)
      {
        common.where();
        return;
      }
    }

  }
}

