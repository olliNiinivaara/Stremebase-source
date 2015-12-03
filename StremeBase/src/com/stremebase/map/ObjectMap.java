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
import java.io.Serializable;
import java.util.stream.LongStream;

import com.stremebase.base.Catalog;
import com.stremebase.base.DynamicMap;
import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;


/**
 * A map to store key-object pairs (the classic key-value store)
 * <p>
 * The object's class must implement {@link java.io.Serializable}
 * <p>
 * ObjectMap does not support indexing or queries by value
 * @author olli
 */
public class ObjectMap extends DynamicMap
{
  @Override
  public void initialize(String mapName, Catalog catalog)
  {
    catalog.setProperty(Catalog.NODESIZE, this, 5);
    catalog.setProperty(Catalog.INITIALCAPACITY, this, 0);
    super.initialize(mapName, catalog);
  }

  @Deprecated
  protected void addIndex(int indexType)
  {
    throw new IllegalArgumentException("ObjectMap does not support indexing.");
  }

  /**
   * Scans through values to find whether a matching serialization exists
   * @param value the object to be searched for
   * @return true if exists
   */
  public boolean containsValue(Serializable value)
  {
    long[] o = serialize(value);
    return keys().filter(key -> listEquals(key, o)).findAny().isPresent();
  }

  @Override
  public long getValueCount(long key)
  {
    return 1;
  }

  /**
   * Returns the object
   * @param key the key
   * @return the object
   */
  public Serializable get(long key)
  {
    return deSerialize(getAsBytes(key));
  }

  /**
   * The serialized byte representation of the object 
   * @param key the key
   * @return the bytes
   */
  public long[] getAsBytes(long key)
  {
    if (!containsKey(key)) return null;
    long[] o = new long[(int)super.getValueCount(key)];
    get(key, o);
    return o;
  }

  /**
   * Stores an object
   * @param key the key
   * @param value the value
   */
  public void put(long key, Serializable value)
  {
    final long[] array =  serialize(value);
    put(key, 0, array);
  }

  /**
   * Stores an object
   * @param key the key
   * @param bytes the serialized object
   */
  public void putBytes(long key, long[] bytes)
  {
    put(key, 0, bytes);
  }

  @Override
  public void remove(long key)
  {
    KeyFile header = getData(key, false);
    if (header==null) return;
    header.setActive(header.base(key), false);
    ValueFile slot = getSlot(key);
    if (slot!=null) releaseSlot(key);
  }

  /**
   * Here value is exceptionally an array of bytes, not a long.
   */
  @Override
  @Deprecated
  public void removeValue(long key, long value)
  {
    throw new UnsupportedOperationException("Value is Object, not long");
  }

  protected long[] serialize(Serializable object)
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

  protected Serializable deSerialize(long[] lArray)
  {
    if (lArray==null) return null;
    byte[] bArray = new byte[lArray.length];
    for (int i=0; i<lArray.length; i++) bArray[i] = (byte)lArray[i];

    ByteArrayInputStream bstream = new ByteArrayInputStream(bArray);
    try
    {
      ObjectInputStream ostream = new ObjectInputStream(bstream);
      return (Serializable) ostream.readUnshared();
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

  /**
   * Serialized objects cannot be streamed as longs.
   */
  @Override
  @Deprecated
  public LongStream values(long key)
  {
    throw new UnsupportedOperationException("Objects have no LongStream of values");
  }

  /**
   * ObjectMap does not support indexing.
   */
  @Override
  @Deprecated
  public void reIndex()
  {
    throw new IllegalArgumentException("ObjectMap does not support indexing.");
  }
}