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
import java.util.OptionalLong;
import java.util.stream.LongStream;
import java.util.stream.LongStream.Builder;

import com.stremebase.base.Catalog;
import com.stremebase.base.DB;
import com.stremebase.base.StremeMap;
import com.stremebase.file.KeyFile;


/**
 * A map for associating a value with a key
 * The value must be converted to long, for example with {@link com.stremebase.base.To}
 */
public class OneMap extends StremeMap
{
  @Override
  public void initialize(String mapName, Catalog catalog)
  {
    catalog.putProperty(Catalog.NODESIZE, this, 2);
    super.initialize(mapName, catalog);
  }

  @Override
  public long getValueCount(long key)
  {
    return 1;
  }

  @Override
  protected void addIndex(DB db, byte indexType)
  {
    if ((indexType != DB.ONE_TO_ONE) && (indexType != DB.MANY_TO_ONE))
      throw new IllegalArgumentException(indexType + "Indextype must be either DB.ONE_TO_ONE or DB.MANY_TO_ONE");
    super.addIndex(db, indexType);
  }

  @Override
  public void reIndex()
  {
    indexer.clear();
    keys().forEach(key -> (indexer.index(key, value())));
    indexer.flush();
  }

  @Override
  public void remove(long key)
  {
    KeyFile buf = getData(key, false);
    if (buf == null) return;
    int base = buf.base(key);
    if (!buf.setActive(base, false)) return;

    if (isIndexed()) indexer.unIndex(key, buf.read(base + 1));
  }

  @Override
  public void removeValue(long key, long value)
  {
    if (get(key)==value) remove(key);
  }

  /**
   * Gets the value
   * @param key the key
   * @return the value
   */
  public long get(long key)
  {
    KeyFile buf = getData(key, false);
    if (buf == null) return DB.NULL;
    int base = buf.base(key);
    if (buf.read(base) == 0) return DB.NULL;
    return buf.read(base + 1);
  }

  /**
   * Returns new key only if the value is unique
   * @param value the value
   * @return new key for new value, old key as negative for existing value
   */
  public long getKeyForUniqueValue(long value)
  {
    OptionalLong result;
    if (!isIndexed()) result = scanningQuery(value, value).findAny();
    else result = indexer.getKeysForValuesInRange(value, value).findAny();
    return -result.orElse(getLargestKey()+1);
  }

  /**
   * Returns the value associated with the key that is currently streamed with keys()
   * <p>
   * Usage:  map.keys().forEach(key -&gt; (map.value()...
   * <p>
   * (Dangerously fails if parallel access)
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
      if (olds) indexer.unIndex(key, buf.read(base + 1));
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