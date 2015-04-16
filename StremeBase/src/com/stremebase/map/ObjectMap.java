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


/**
 * A map to store key-object pairs (the classic key-value store)
 * The object's class must implement {@link java.io.Serializable}
 * ObjectMap does not support queries by value
 */
public class ObjectMap extends DynamicMap
{  
  /**
   * Creates a new ObjectMap, which is persisted iff DB is
   * @param mapName the name
   */
  public ObjectMap(String mapName)
  {
    super (mapName, 0, DB.isPersisted());
  }

  /**
   * Creates a new ObjectMap
   * @param mapName the name
   * @param persisted if persisted
   */
  public ObjectMap(String mapName, boolean persisted)
  {
    super (mapName, 0, persisted);
  }

  @Deprecated
  public void addIndex(int indexType)
  {
    throw new IllegalArgumentException("ObjectMap does not support indexing.");
  }

  /**
   * Scans through values to find whether a matching serialization exists
   * @param value the object to be searched for
   * @return true if exists
   */
  public boolean containsValue(Object value)
  {
    long[] o = serialize(value);
    return keys().filter(key -> listEquals(key, o)).findAny().isPresent();
  }

  /**
   * Returns the object
   * @param key the key
   * @return the object
   */
  public Object get(long key)
  {
    if (!containsKey(key)) return null;
    long[] o = new long[(int)getSize(key)];
    get(key, o);
    return deSerialize(o);
  }

  /**
   * Stores an object
   * @param key the key
   * @param value the value
   */
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
  @Deprecated
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    throw new UnsupportedOperationException("Objects cannot be queried");
  }

  @Override
  @Deprecated
  protected LongStream scanningUnionQuery(long... values)
  {
    throw new UnsupportedOperationException("Objects cannot be queried");
  }

  @Override
  @Deprecated
  public LongStream values(long key)
  {
    throw new UnsupportedOperationException("Objects have no LongStream of values");
  }

  @Override
  @Deprecated
  public void reIndex()
  {
    throw new IllegalArgumentException("ObjectMap does not support indexing.");
  }
}