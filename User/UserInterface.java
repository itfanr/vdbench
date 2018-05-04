package User;
    
/*  
 * Copyright (c) 2000, 2012, Oracle and/or its affiliates. All rights reserved. 
 */ 
    
/*  
 * Author: Henk Vandenbergh. 
 */ 

import Vdb.Cmd_entry;
import Vdb.SlaveJvm;
import Vdb.common;

public interface UserInterface
{

  /**
   * Parse the input parameters specified in the parameter file.
   *
   * This method is called twice: once in the Vdbench Master, solely for parameter
   * validation, and then again on the Vdbench Slave immediately after the
   * instance of this class has been created.
   *
   * Any data stored in this current instance will be lost when called from the
   * master, however, when called from a slave this data will be available for ont
   * current Run Definitoon (RD).
   */
  public boolean parser(String[] parms);

  /**
   * Preparation for the generation of a workload.
   * Called once per WD/SD pair per run.
   */
  public UserDeviceInfo initialize(WorkloadInfo wi);

  /**
   * Generate workload. Do not return until the last i/o has been scheduled, or
   * until SlaveJvm.isWorkloadDone().
   */
  public boolean generate();

  /**
   * Called before and after an i/o is sent to JNI code.
   *
   * I should replace Cmd_entry with something more 'hidden'!
   */
  public boolean preIO(UserCmd ucmd);

  public boolean postIO(UserCmd ucmd);

  public WorkloadInfo getWorkloadInfo();
  public void setWorkloadInfo(WorkloadInfo wi);

  /**
   * Let Vdbench know if the generate method needs to be called.
   */
  public boolean isGenerateNeeded();

  public boolean isWorkloadDone();

  public boolean isThisSlave();

  /**
   * Lock to be used when users want to manipulate the user defined shared_object.
   */
  public Object getSharedLock();

  public void setSharedObject(Object s);
  public Object getSharedObject();

  public Object[] getDeviceList();
}
