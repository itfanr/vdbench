package Vdb;
import java.io.IOException;
import java.io.ObjectInputStream;


/**
 * Object created to allow automatic counting and reporting of the amount of
 * instances that currently exist.
 *
 * As long as an Object extends VdbObject, counting is automatic.
 * For an Object that already extends something else, like 'extends Thread',
 * just code 'VdbCount.count(this);' during instantiation. In that case you'll
 * also have to code a clone() and readObject() method including this call.
 *
 **/
public class VdbObject extends Object
{
  private final static String c = "Copyright (c) 2010 Sun Microsystems, Inc. " +
                                  "All Rights Reserved. Use is subject to license terms.";



  public VdbObject()
  {
    VdbCount.count(this);
  }


  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
  {
    in.defaultReadObject();
    VdbCount.count(this);
  }


  public Object clone()
  {

    try
    {
      Object clone = super.clone();
      VdbCount.count(clone);

      return clone;
    }
    catch (Exception e)
    {
      common.failure(e);
    }
    return null;
  }

  public void finalize() throws Throwable
  {
    VdbCount.sub(this);
    super.finalize();
  }
}
