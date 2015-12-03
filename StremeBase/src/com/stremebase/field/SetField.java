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

import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.stremebase.dal.Field;
import com.stremebase.dal.Table;
import com.stremebase.dal.Value;
import com.stremebase.map.SetMap;

/**
 * A field that stores a set of values
 * @author olli
 *
 * @param <T> the Class of the values
 */
public class SetField<T> extends Field<T>
{
  public final SetMap setMap;
  public final Class<?> valueClass;

  /**
   * Defines the set field
   * @param table the table
   * @param name the name
   * @param valueClass the Class of the values
   */
  public SetField(Table table, String name, Class<?> valueClass)
  {
    super(table, name);
    assertValidValueClass(valueClass);
    this.valueClass = valueClass;
    table.tableDb.defineMap(table.name+"_"+name, SetMap.class);
    map = table.tableDb.getMap(table.name+"_"+name);
    setMap = (SetMap)map;
  }

  /**
   * Returns the low-level representations of values as an array, to be used when querying
   * @param key the key
   * @return the values
   */
  public long[] getAsLongArray(long key)
  {
    return setMap.values(key).toArray();
  }

  /**
   * Returns the values as stream
   * @param key the key
   * @return stream of values
   */
  @SuppressWarnings("unchecked")
  public Stream<T> getAsStream(long key)
  {
    return (Stream<T>) setMap.values(key).mapToObj(value -> Value.asObject(value, valueClass));
  }

  /**
   * Returns the low-level representations of values as a stream, to be used when querying
   * @param key the key
   * @return the values
   */
  public LongStream values(long key)
  {
    return setMap.values(key);
  }

  protected void add(long key, long value)
  {
    setMap.put(key, value);
    setModified(true);
  }

  /**
   * Adds a value to a set
   * @param key the key of the set
   * @param value the value to be added
   */
  public void add(long key, Object value)
  {
    setMap.put(key, Value.asLong(value));
    setModified(true);
  }
}
