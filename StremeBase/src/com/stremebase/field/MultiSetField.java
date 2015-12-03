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

package com.stremebase.field;

import java.util.stream.Stream;

import com.stremebase.dal.Field;
import com.stremebase.dal.Table;
import com.stremebase.dal.Value;
import com.stremebase.map.SetMap;
import com.stremebase.map.SetMap.SetEntry;

/**
 * A field that that stores a multiset of values
 * @author olli
 *
 * @param <T> the Class of the values
 */
public class MultiSetField<T> extends Field<T>
{
  /**
   * Includes the count of the value
   * @author olli
   *
   * @param <T> the Class of the value
   */
  public static class MultiSetEntry<T>
  {
    long key;
    T value;
    long count;

    public MultiSetEntry(long key, T value, long count)
    {
      this.key = key;
      this.value = value;
      this.count = count;
    }
  }

  public final SetMap setMap;
  public final Class<?> valueClass;

  /**
   * Defines a multiset field
   * @param table table of this field
   * @param name name of the field
   * @param valueClass Class of the values
   */
  public MultiSetField(Table table, String name, Class<?> valueClass)
  {
    super(table, name);
    assertValidValueClass(valueClass);
    this.valueClass = valueClass;
    table.tableDb.defineMultiSetMap(table.name+"_"+name);
    map = table.tableDb.getMap(table.name+"_"+name);
    setMap = (SetMap)map;
  }

  /**
   * Stream of the values and their counts associated with the key
   * @param key the key
   * @return the stream
   */
  @SuppressWarnings("unchecked")
  public Stream<MultiSetEntry<T>> getAsStream(long key)
  {
    return setMap.entries(key).map(e -> new MultiSetEntry<T>(e.key, (T) Value.asObject(e.value, valueClass), e.attribute));
  }

  protected void add(long key, long... values)
  {
    for (long value: values) setMap.put(key, value);
    setModified(true);
  }

  /**
   * Adds values
   * @param key the key
   * @param values the values
   */
  public void add(long key, @SuppressWarnings("unchecked") T... values)
  {
    add(key, Value.asLongArray(values));
  }

  /**
   * The low-level stream of values (where values are longs), to be used when querying
   * @param key the key
   * @return the stream
   */
  public Stream<SetEntry> entries(long key)
  {
    return setMap.entries(key);
  }

  protected void add(long key, long value, int count)
  {
    setMap.put(key, value, count);
    setModified(true);
  }

  /**
   * Adds (or subtracts) multiple occurrences at once 
   * @param key the key
   * @param value the value
   * @param count amount of the value to add
   */
  public void add(long key, T value, int count)
  {
    setMap.put(key, Value.asLong(value), count);
    setModified(true);
  }

  /**
   * Adds values as entries
   * @param entries the entries
   */
  public void add(@SuppressWarnings("unchecked") MultiSetEntry<T>... entries)
  {
    for (MultiSetEntry<T> e: entries) add(e.key, Value.asLong(e.value), e.count);
  }
}
