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
    super(mapName, 2, DB.NOINDEX, DB.Persisted());
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

    if (isIndexed())
      indexer.index(key, buf.read(base + 1), DB.NULL);
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
   * Associates a value with a key
   * 
   * @param key
   *          the key
   * @param value
   *          the value
   */
  public void put(long key, long value)
  {
    if (key < 0)
      throw new IllegalArgumentException("Negative keys are not supported ("
          + key + ")");
    KeyFile buf = getData(key, true);
    int base = buf.base(key);
    boolean olds = !buf.setActive(base, true);
    if (isIndexed())
    {
      long oldValue = DB.NULL;
      if (olds)
        oldValue = buf.read(base + 1);
      indexer.index(key, oldValue, value);
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
    if (buf == null)
      return LongStream.empty();
    int base = buf.base(key);
    Builder b = LongStream.builder();
    b.add(buf.read(base + 1));
    return b.build();
  }

  @Override
  protected Object getObject(long key)
  {
    return get(key);
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    Builder b = LongStream.builder();
    keys().filter(key -> {
      long value = get(key);
      if (value < lowestValue || value > highestValue)
        return false;
      return true;
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