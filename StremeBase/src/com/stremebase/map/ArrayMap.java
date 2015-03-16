package com.stremebase.map;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;
import com.stremebase.base.DB;
import com.stremebase.base.FixedMap;
import com.stremebase.base.Indexer;
import com.stremebase.file.KeyFile;

public class ArrayMap extends FixedMap
{
  protected final Map<Integer, Indexer> indices = new HashMap<>();
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
    super(mapName, size+1, DB.isPersisted());
  }

  public ArrayMap(String mapName, int size, boolean persist)
  {
    super(mapName, size+1, persist);
  }

  public int getArrayLength()
  {
    return this.getNodeSize()-1;
  }

  public void addIndex(int indexType)
  {
    if (indexer!=null) return;
    if (indexType == DB.ONE_TO_ONE || indexType == DB.MANY_TO_ONE)
      throw new IllegalArgumentException("This indextype can be used only for single cells (method addIndextoCell)");

    indexer = new Indexer(indexType, this);
  }

  public void addIndextoCell(int indexType, int cell)
  {
    if (indices.containsKey(cell)) return;
    if (indexType != DB.ONE_TO_ONE && indexType != DB.MANY_TO_ONE)
      throw new IllegalArgumentException("For single cells, indextype must be either DB.ONE_TO_ONE or DB.MANY_TO_ONE");

    indices.put(cell, new Indexer(indexType, this, cell));
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

    if (isIndexed()) values(key).forEach(value -> indexer.remove(key, value));
    for (int cell: indices.keySet()) indices.get(cell).remove(key, get(key, cell));

    int base = buf.base(key);
    buf.setActive(base, false);
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

  @Override
  public void put(long key, int index, long value)
  {
    if (key < 0) throw new IllegalArgumentException("Negative keys are not supported (" + key + ")");
    if (index < 0 || index>this.nodeSize) throw new IllegalArgumentException("Index out of range (" + index + ")");
    KeyFile buf = getData(key, true);
    int base = buf.base(key);
    boolean olds = !buf.setActive(base, true);
    if (isIndexed() || indices.containsKey(index))
    {
      long oldValue = DB.NULL;
      if (olds) oldValue = buf.read(base + index + 1);
      if (isIndexed())
      {
        if (oldValue!=DB.NULL) indexer.remove(key, oldValue);
        if (value!=DB.NULL) indexer.index(key, value);
      }

      Indexer celli = indices.get(index);
      if (celli!=null)
      {
        if (oldValue!=DB.NULL) celli.remove(key, oldValue);
        if (value!=DB.NULL) celli.index(key, value);
      }
    }
    buf.write(base+1+index, value);
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
    LongStream.Builder b = LongStream.builder();
    for (int i=1; i<nodeSize; i++) b.add(buf.read(base + i));
    return b.build();
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    //TODO: read arrays
    LongStream.Builder b =  LongStream.builder();

    keys().filter(key ->
    {
      return values(key).anyMatch(value -> (value!= DB.NULL && value >= lowestValue && value<=highestValue));
    }).forEach(key -> b.add(key));

    return b.build();
  }

  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    //TODO: read arrays

    Arrays.sort(values);
    LongStream.Builder b =  LongStream.builder();

    keys().filter(key ->
    {
      return values(key).anyMatch(value -> (Arrays.binarySearch(values, value)>=0 ? true : false));
    }).forEach(key -> b.add(key));

    return b.build();
  }

  @Override
  protected int indexOf(long key, int fromIndex, long value)
  {
    throw new UnsupportedOperationException("TBD");
  }
}
