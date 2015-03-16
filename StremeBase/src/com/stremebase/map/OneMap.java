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

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.LongStream.Builder;

import com.stremebase.base.DB;
import com.stremebase.base.FixedMap;
import com.stremebase.file.KeyFile;


public class OneMap extends FixedMap
{
  /**
   * Creates a new OneMap for associating one value with one key.
   * The returned map is not indexed.
   * The returned map is persistent iff the database is.
   * 
   * @param mapName
   *          name for the map. Must be a database-wide unique value.
   */
  public OneMap(String mapName)
  {
    super(mapName, 2, DB.isPersisted());
  }

  public OneMap(String mapName, boolean persist)
  {
    super(mapName, 2, persist);
  }

  public void addIndex(int indexType)
  {
    if ((indexType != DB.ONE_TO_ONE) && (indexType != DB.MANY_TO_ONE))
      throw new IllegalArgumentException(indexType + "Indextype must be either DB.ONE_TO_ONE or DB.MANY_TO_ONE");
    super.addIndex(indexType);
  }

  /**
   * Removes the given key from the map
   * 
   * @param key
   *          the key to be removed
   */
  public void remove(long key)
  {
    KeyFile buf = getData(key, false);
    if (buf == null)
      return;
    int base = buf.base(key);
    if (!buf.setActive(base, false))
      return;

    if (isIndexed()) indexer.remove(key, buf.read(base + 1));
  }

  /**
   * Returns the value associated with the key
   * 
   * @param key
   *          the key
   * @return the value, or {@link com.stremebase.base.DB#NULL} if key was
   *         nonexistent
   */
  public long get(long key)
  {
    KeyFile buf = getData(key, false);
    if (buf == null)
      return DB.NULL;
    int base = buf.base(key);
    if (buf.read(base) == 0)
      return DB.NULL;
    return buf.read(base + 1);
  }

  /**
   * Returns the value associated with the key that is currently streamed with keys()
   * Usage:  map.keys().forEach(key -&gt; (map.value()...
   * (Definitely not thread safe)
   * @return the currently iterated value
   */
  public long value()
  {
    return iteratedValue;
  }

  /**
   * Associates a value with a key
   * 
   * @param key
   *          the key
   * @param value
   *          the value
   */
  public void put(long key, long value)
  {
    if (key < 0) throw new IllegalArgumentException("Negative keys are not supported (" + key + ")");
    KeyFile buf = getData(key, true);
    int base = buf.base(key);
    boolean olds = !buf.setActive(base, true);
    if (isIndexed())
    {
      if (olds) indexer.remove(key, buf.read(base + 1));
      indexer.index(key, value);
    }
    buf.write(base + 1, value);
  }

  /**
   * Returns the value associated with a key as a {@link LongStream}
   * 
   * @param key
   *          the key
   * @return the value or an empty stream if there's no value
   */
  @Override
  public LongStream values(long key)
  {
    KeyFile buf = getData(key, false);
    if (buf == null) return LongStream.empty();
    int base = buf.base(key);
    Builder b = LongStream.builder();
    b.add(buf.read(base + 1));
    return b.build();
  }

  /*@Override
  protected Object getObject(long key)
  {
    return get(key);
  }*/

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    Builder b = LongStream.builder();
    keys().filter(key ->
    {
      if (iteratedValue < lowestValue || iteratedValue > highestValue) return false;
      return true;
    }).forEach(key -> b.add(key));
    return b.build();
  }

  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    Arrays.sort(values);    
    Builder b = LongStream.builder();
    keys().filter(key ->
    {
      return Arrays.binarySearch(values, iteratedValue)>=0 ? true : false;
    }).forEach(key -> b.add(key));
    return b.build();
  }

  @Override
  protected void put(long key, int index, long value)
  {
    throw new UnsupportedOperationException("OneMap index?");
  }

  @Override
  protected int indexOf(long key, int fromIndex, long value)
  {
    throw new UnsupportedOperationException("OneMap index?");
  }
}