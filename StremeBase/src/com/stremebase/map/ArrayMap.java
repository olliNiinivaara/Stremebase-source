package com.stremebase.map;

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.LongStream.Builder;

import com.stremebase.base.DB;
import com.stremebase.base.FixedMap;
import com.stremebase.file.KeyFile;

public class ArrayMap extends FixedMap
{

  /**
   * Creates a new ArrayMap for associating an array of values with one key.
   * The returned map is not indexed.
   * The returned map is persistent iff the database is.
   * 
   * @param mapName
   *          name for the map. Must be a database-wide unique value.
   */
  public ArrayMap(String mapName, int size)
  {
    super(mapName, size+1, DB.NOINDEX, DB.isPersisted());
  }
  
  public ArrayMap(String mapName, int size, boolean persist)
  {
    super(mapName, size+1, DB.NOINDEX, persist);
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
    if (buf == null) return;
    int base = buf.base(key);
    if (!buf.setActive(base, false)) return;

    if (isIndexed()) throw new UnsupportedOperationException("indexing TBD");
      //indexer.index(key, buf.read(base + 1), DB.NULL);
  }

  /**
   * Returns the values associated with the key to the given array
   * 
   * @param key
   *          the key
   *  @param values an array where the values are written in. Must be large enough.     
   * @return true, if values exist
   */
  public boolean get(long key, long[] values)
  {
    KeyFile buf = getData(key, false);
    if (buf == null) return false;
    int base = buf.base(key);
    if (buf.read(base) == 0) return false;
    buf.readToArray(base+1, values, nodeSize-1);
    return true;
  }
  
  /**
   * Returns the values associated with the key, or null
   * 
   * @param key
   *          the key  
   * @return values, or null if there are no values
   */
  public long[] get(long key)
  {
    long[] values = new long[nodeSize-1];
    if (!get(key, values)) return null;
    return values;
  }
  
  public long get(long key, int index)
  {
    KeyFile buf = getData(key, false);
    if (buf == null) return DB.NULL;
    int base = buf.base(key);
    if (buf.read(base) == 0) return DB.NULL;
    return buf.read(base+1+index);
  }
  
  /*
   * Returns the value associated with the key that is currently streamed with keys()
   * Usage:  map.keys().forEach(key -&gt; (map.value()...
   * (Definitely not thread safe)
   * @return the currently iterated value
   *
  public long value()
  {
    return iteratedValue;
  }*/

  /**
   * Associates values with a key
   * 
   * @param key
   *          the key
   * @param values
   *          the values
   */
  public void put(long key, long[] values)
  {
    if (key < 0) throw new IllegalArgumentException("Negative keys are not supported (" + key + ")");
    KeyFile buf = getData(key, true);
    int base = buf.base(key);
    buf.setActive(base, true);
    /*boolean olds = !buf.setActive(base, true);
    if (isIndexed())
    {
      long oldValue = DB.NULL;
      if (olds) oldValue = buf.read(base + 1);
      indexer.index(key, oldValue, value);
    }*/
    buf.write(base+1, values);
  }

  /*
   * Returns the value associated with a key as a {@link LongStream}
   * 
   * @param key
   *          the key
   * @return the value or an empty stream if there's no value
   *
  @Override
  public LongStream values(long key)
  {
    KeyFile buf = getData(key, false);
    if (buf == null) return LongStream.empty();
    int base = buf.base(key);
    Builder b = LongStream.builder();
    b.add(buf.read(base + 1));
    return b.build();
  }*/
  
  @Override
  public LongStream values(long key)
  {
    throw new UnsupportedOperationException("TBD");
  }

  @Override
  protected Object getObject(long key)
  {
    return get(key);
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    throw new UnsupportedOperationException("TBD");
    /*Builder b = LongStream.builder();
    keys().filter(key ->
    {
      if (iteratedValue < lowestValue || iteratedValue > highestValue) return false;
      return true;
    }).forEach(key -> b.add(key));
    return b.build();*/
  }
  
  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    throw new UnsupportedOperationException("TBD");
    /*Arrays.sort(values);    
    Builder b = LongStream.builder();
    keys().filter(key ->
    {
      return Arrays.binarySearch(values, iteratedValue)>=0 ? true : false;
    }).forEach(key -> b.add(key));
    return b.build();*/
  }

  @Override
  protected void put(long key, int index, long value)
  {
    throw new UnsupportedOperationException("TBD");
  }

  @Override
  protected int indexOf(long key, int fromIndex, long value)
  {
    throw new UnsupportedOperationException("TBD");
  }
}
