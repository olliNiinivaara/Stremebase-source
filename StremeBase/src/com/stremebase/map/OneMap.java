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
import com.stremebase.base.StremeMap;
import com.stremebase.file.KeyFile;


/**
 * A map for associating a value with a key
 * The value must be converted to long, for example with {@link com.stremebase.base.To}
 */
public class OneMap extends StremeMap
{
  /**
   * Creates a new OneMap for associating one value for each key.
   * The returned map is persistent iff the database is.
   *
   * @param mapName
   *          name for the map. Must be a database-wide unique value.
   */
  public OneMap(String mapName)
  {
    super(mapName, 2, DB.isPersisted());
  }

  /**
   * Creates a new OneMap for associating one value with one key.
   * The returned map is persistent iff the database is.
   *
   * @param mapName
   *          name for the map. Must be a database-wide unique value.
   * @param persist if persisted         
   */
  public OneMap(String mapName, boolean persist)
  {
    super(mapName, 2, persist);
  }

  @Override
  public void addIndex(byte indexType)
  {
    if ((indexType != DB.ONE_TO_ONE) && (indexType != DB.MANY_TO_ONE))
      throw new IllegalArgumentException(indexType + "Indextype must be either DB.ONE_TO_ONE or DB.MANY_TO_ONE");
    super.addIndex(indexType);
  }

  @Override
  public void reIndex()
  {
    indexer.clear();
    keys().forEach(key -> (indexer.index(key, value())));
    indexer.commit();
  }

  @Override
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

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    return keys().filter(key ->
    {
      if (iteratedValue < lowestValue || iteratedValue > highestValue) return false;
      return true;
    });
  }

  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    Arrays.sort(values);

    return keys().filter(key ->
    {
      return Arrays.binarySearch(values, iteratedValue)>=0 ? true : false;
    });
  }

  @Override
  protected void put(long key, int index, long value)
  {
    throw new UnsupportedOperationException("OneMap index?");
  }
}