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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

import com.stremebase.base.DB;
import com.stremebase.base.StremeMap;
import com.stremebase.base.Indexer;
import com.stremebase.file.KeyFile;

public class ArrayMap extends StremeMap
{
  protected final Map<Integer, Indexer> indices = new HashMap<>();


  /**
   * Creates a new ArrayMap for associating a fixed-size array of values with one key.
   * Supports cell-specific indexing.
   * The returned map is persistent iff the database is.
   *
   * @param mapName
   *          name for the map. Must be a database-wide unique value.
   *  @param size Size of the array         
   */
  public ArrayMap(String mapName, int size)
  {
    super(mapName, size+1, DB.isPersisted());
  }

  /**
   * Creates a new ArrayMap for associating a fixed-size array of values with one key.
   * Supports cell-specific indexing.
   * The returned map is persistent iff the database is.
   *
   * @param mapName
   *          name for the map. Must be a database-wide unique value.
   *  @param size Size of the array 
   *  @param persist whether the map is persisted        
   */
  public ArrayMap(String mapName, int size, boolean persist)
  {
    super(mapName, size+1, persist);
  }

  /**
   * Returns the size of array (same for all keys)
   * @return the length
   */
  public int getArrayLength()
  {
    return this.getNodeSize()-1;
  }

  @Override
  public void addIndex(byte indexType)
  {
    if (indexType == DB.ONE_TO_ONE || indexType == DB.MANY_TO_ONE)
      throw new IllegalArgumentException("This indextype can be used only for single cells (method addIndextoCell)");
    super.addIndex(indexType);
  }

  @Override
  public void reIndex()
  {
    indexer.clear();
    keys().forEach(key -> (index(key, null, get(key))));
    indexer.commit();
  }

  @Override
  public void commit()
  {
    for (Indexer i: indices.values()) i.commit();
    super.commit();
  }

  @Override
  public void close()
  {
    for (Indexer i: indices.values()) i.close();
    super.close();
  }

  @Override
  public void clear()
  {
    for (Indexer i: indices.values()) i.clear();
    super.clear();
  }

  /**
   * Adds an index to a specific cell
   * @param indexType the index type (either DB.ONE_TO_ONE or DB.MANY_TO_ONE)
   * @param cell the index of the cell
   */
  public void addIndextoCell(byte indexType, int cell)
  {
    if (indices.containsKey(cell)) return;
    if (indexType != DB.ONE_TO_ONE && indexType != DB.MANY_TO_ONE)
      throw new IllegalArgumentException("For single cells, indextype must be either DB.ONE_TO_ONE or DB.MANY_TO_ONE");

    Indexer cIndexer = new Indexer(indexType, this, cell);
    indices.put(cell, cIndexer);
    if (!isEmpty() && (cIndexer.isEmpty())) reIndexCell(cell);
  }

  /**
   * Reindexes cell-specific index
   * Mainly for internal use.
   * @param cell the index of the index
   */
  public void reIndexCell(int cell)
  {
    Indexer cIndexer = indices.get(cell);
    cIndexer.clear();
    keys().forEach(key -> (cIndexer.index(key, get(key, cell))));
    cIndexer.commit();
  }

  /**
   * Remoces an index
   * @param cell the index of the index
   */
  public void dropIndexFromCell(int cell)
  {
    Indexer i = indices.get(cell);
    if (i == null) return;
    i.clear();
    indices.put(cell, null);
  }

  protected void index(long key, long[] oldValues, long[] newValues)
  {
    if (oldValues!=null) for (long v: oldValues) indexer.remove(key, v);
    if (newValues!=null) for (long v: newValues) indexer.index(key, v);
  }

  @Override
  public void remove(long key)
  {
    KeyFile buf = getData(key, false);
    if (buf == null) return;

    if (isIndexed()) index(key, get(key), null);
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

  /**
   * Returns the value at given cell
   * @param key the key
   * @param index the cell
   * @return value or DB.NULL
   */
  public long get(long key, int index)
  {
    KeyFile buf = getData(key, false);
    if (buf == null) return DB.NULL;
    int base = buf.base(key);
    if (buf.read(base) == 0) return DB.NULL;
    return buf.read(base+1+index);
  }

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
    long[] oldValues = null;
    if (!buf.setActive(base, true)) if (isIndexed()) oldValues = get(key);
    if (isIndexed()) index(key, oldValues, values);
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
    return keys().filter(key ->
    {
      //TODO: bulk read arrays?
      return values(key).anyMatch(value -> (value!= DB.NULL && value >= lowestValue && value<=highestValue));
    });
  }

  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    Arrays.sort(values);

    return keys().filter(key ->
    {
      return values(key).anyMatch(value -> (Arrays.binarySearch(values, value)>=0 ? true : false));
    });
  }

  protected int indexOf(long key, int fromIndex, long value)
  {
    if (fromIndex<0 || fromIndex>getArrayLength()-1) throw new IndexOutOfBoundsException("Index out of bounds: "+fromIndex);
    KeyFile buf = getData(key, false);
    if (buf == null) return -1;
    int base = buf.base(key);
    if (buf.read(base) == 0) return -1;    
    for (int i = fromIndex; i < getArrayLength(); i++) if (buf.read(base+1+i)==value) return i;
    return -1;
  }

  /**
   * A range query that considers only values at a given index
   * @param index the cell
   * @param lowestValue lowest acceptable value
   * @param highestValue highest acceptable value
   * @return matching keys
   */
  public LongStream queryByCell(int index, long lowestValue, long highestValue)
  {
    if (!indices.containsKey(index)) return scanningQueryByCell(index, lowestValue, highestValue);
    if (isIndexQuerySorted()) return indices.get(index).getKeysWithValueFromRange(lowestValue, highestValue).sorted();
    return indices.get(index).getKeysWithValueFromRange(lowestValue, highestValue);
  }

  protected LongStream scanningQueryByCell(int index, long lowestValue, long highestValue)
  {
    return keys().filter(key ->
    {
      long value = get(key, index);
      return value!= DB.NULL && value >= lowestValue && value<=highestValue;
    });
  }

  /**
   * A OR-query that considers only values at a given index
   * @param index the cell
   * @param values the acceptable values
   * @return matching keys
   */
  public LongStream unionQueryByCell(int index, long...values)
  {
    if (!indices.containsKey(index)) return scanningUnionQueryByCell(index, values);
    if (isIndexQuerySorted()) return indices.get(index).getKeysWithValueFromSet(values).sorted();
    return indices.get(index).getKeysWithValueFromSet(values);
  }


  protected LongStream scanningUnionQueryByCell(int index, long... values)
  {
    Arrays.sort(values);

    return keys().filter(key ->
    {
      long value = get(key, index);
      if (value==DB.NULL) return false;
      return Arrays.binarySearch(values, value)>=0 ? true : false;
    });
  }
}
