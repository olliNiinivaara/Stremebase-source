/*
 * ---------------------------------------------------------
 * BEER-WARE LICENSED
 * This file is based on original work by Olli Niinivaara.
 * As long as you retain this notice you can do whatever
 * you want with this stuff. If you meet him one day, and
 * you think this stuff is worth it, you can buy him a
 * beer in return.
 * ---------------------------------------------------------
 */

package com.stremebase.map;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.stream.LongStream;

import com.stremebase.base.DB;
import com.stremebase.base.DynamicMap;


public class ObjectMap extends DynamicMap
{		
  public ObjectMap(String mapName)
  {
    super (mapName, 10000, DB.isPersisted());
  }

  public boolean containsValue(Object value)
  {
    long[] o = serialize(value);
    return keys().filter(key -> listEquals(key, o)).findAny().isPresent();
  }

  public Object get(long key)
  {
    if (!containsKey(key)) return null;
    long[] o = new long[getSize(key)];
    get(key, o);
    return deSerialize(o);
  }

  public void put(long key, Object value)
  {
    final long[] array =  serialize(value);
    put(key, 0, array);
  }

  protected long[] serialize(Object object)
  {
    ByteArrayOutputStream baoStream = new ByteArrayOutputStream();
    try
    {
      ObjectOutputStream ooStream = new ObjectOutputStream(baoStream);
      ooStream.writeUnshared(object);
      ooStream.close();
      baoStream.close();
      byte[] bArray = baoStream.toByteArray();
      long[] lArray = new long[bArray.length];
      for (int i=0; i<bArray.length; i++) lArray[i] = bArray[i];
      return lArray;
    }
    catch (IOException e)
    {
      throw new IllegalStateException(e);
    }
  }

  protected Object deSerialize(long[] lArray)
  {
    if (lArray==null) return null;
    byte[] bArray = new byte[lArray.length];
    for (int i=0; i<lArray.length; i++) bArray[i] = (byte)lArray[i];

    ByteArrayInputStream bstream = new ByteArrayInputStream(bArray);
    try
    {
      ObjectInputStream ostream = new ObjectInputStream(bstream);
      return ostream.readUnshared();
    }
    catch (IOException | ClassNotFoundException e)
    {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    throw new UnsupportedOperationException("Objects cannot be queried");
  }

  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    throw new UnsupportedOperationException("Objects cannot be queried");
  }

  @Override
  public LongStream values(long key)
  {
    throw new UnsupportedOperationException("Objects have no LongStream of values");
  }
}