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
import com.stremebase.base.DB;
import com.stremebase.base.DynamicMap;
import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;

/**
 * A map for associating a dynamically expanding array of values with a key.
 * Supports stack operations push and pop, but insertions to middle of list.
 * Removing from middle can be simulated by overwriting with DB.NULL.
 */
public class StackListMap extends DynamicMap
{
  public boolean skipNulls = false;

  @Override
  public void reIndex()
  {
    indexer.clear();
    keys().forEach(key -> (values(key).forEach(value -> (indexer.index(key, value)))));
    indexer.flush();
  }

  /**
   * Moves the head of the stack backwards
   * @param key the key
   * @param newSize new, smaller size
   */
  public void shrinkValueSize(long key, int newSize)
  {
    if (key==DB.NULL) return;
    KeyFile header = getData(key, false);
    if (header==null) return;
    int oldSize = (int) header.read(header.base(key)+pLength);
    if (newSize>=oldSize) return;
    header.write(header.base(key)+pLength, newSize);
  }

  /**
   * Pushes to end of the stack
   * @param key the key
   * @param value the value
   */
  public void push(long key, long value)
  {
    int position = (int) getValueCount(key);
    super.put(key, position, value);
  }

  /**
   * Pushes to the end of stack
   * @param key the key
   * @param values the values
   */
  public void push(long key, long... values)
  {
    int position = (int) getValueCount(key);
    for (int i=0; i<values.length; i++) super.put(key, position+i, values[i]);
  }

  /**
   * Pops from stack
   * @param key key
   * @return value or DB.NULL
   */
  public long pop(long key)
  {
    int size = (int) getValueCount(key);
    if (size==0) return DB.NULL;
    int position = size-1;
    while (position >= 0)
      if (super.get(key, position)==DB.NULL) position--;
      else
      {
        shrinkValueSize(key, position+1);
        if (position==-1) return DB.NULL; else return super.get(key, position);
      }
    shrinkValueSize(key, 0);
    return DB.NULL;
  }

  /**
   * Overwrites at index
   */
  @Override
  public void put(long key, int index, long value)
  {
    super.put(key, index, value);
    if (index==getValueCount(key)-1 && value==DB.NULL)
    {
      while (index >= 0) if (super.get(key, index)==DB.NULL) index--;
      shrinkValueSize(key, index+1);
    }
  }

  /**
   * Overwrites
   */
  @Override
  public void put(long key, int index, long... values)
  {
    for (int i = 0; i<values.length; i++) put(key, index+i+1, values[i]);
  }

  @Override
  public long get(long key, int index)
  {
    return super.get(key, index);
  }

  /**
   * The contents of the stack, skipping DB.NULLs
   * @param key the key
   * @return the stack
   */
  public long[] get(long key)
  {
    KeyFile header = getData(key, false);
    if (header==null) return null;
    int base = header.base(key);
    if (header.read(base)==0) return null;
    ValueFile file = fileManager.getValueFile(mapGetter, header.read(base+pSlotFileId));
    int length = (int)header.read(base+pLength);
    long[] result = new long[length];
    file.readToArray((int)header.read(base+pSlotFilePosition), result, length);
    return result;
  }

  @Override
  public void removeValue(long key, long value)
  {
    if (value==DB.NULL) return;

    while (true)
    {
      int index = indexOf(key, 0, value);
      if (index==-1) return;
      if (index==getValueCount(key)-1) pop(key); else put(key, index, DB.NULL);
    }
  }

  @Override
  public LongStream values(long key)
  {
    return super.values(key, skipNulls);
  }

  /**
   * Returns the first index of value, starting from fromIndex
   * or -1 if not found
   */
  @Override
  public int indexOf(long key, int fromIndex, long value)
  {
    return super.indexOf(key, fromIndex, value);
  }

  /**
   * Search for all values
   * @param key the key
   * @param values the values
   * @return true iff all the values exist
   */
  public boolean containsValues(long key, long... values)
  {
    long[] list = get(key);
    if (list.length<values.length) return false;
    Arrays.sort(list);
    if (values.length>5000) return LongStream.of(values).parallel().filter(value->value!=DB.NULL).allMatch(value -> (Arrays.binarySearch(list, value)>=0 ? true : false));
    else return LongStream.of(values).filter(value->value!=DB.NULL).allMatch(value -> (Arrays.binarySearch(list, value)>=0 ? true : false));
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    return keys().filter(key ->
    {
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
}
