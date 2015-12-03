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
import com.stremebase.map.StackListMap;

/**
 * A field to store a stack of values
 * @author olli
 *
 * @param <T> the Class of the values
 */
public class StackField<T> extends Field<T>
{
  public final StackListMap stackListMap;
  public final Class<?> valueClass;

  /**
   * Defines the field
   * @param table the table
   * @param name the name
   * @param valueClass the Class of the values
   */
  public StackField(Table table, String name, Class<?> valueClass)
  {
    super(table, name);
    assertValidValueClass(valueClass);
    this.valueClass = valueClass;
    table.tableDb.defineMap(table.name+"_"+name, StackListMap.class);
    map = table.tableDb.getMap(table.name+"_"+name);
    stackListMap = (StackListMap)map;
  }

  /**
   * Returns the low-level representations of the values as a stream skipping nulls, to be used in queries
   * @param key the key
   * @return the values
   */
  public LongStream getAsLongStream(long key)
  {
    return stackListMap.values(key);
  }

  /**
   * Gets the values as a stream skipping nulls
   * @param key the key
   * @return the values
   */
  public Stream<T> getAsStream(long key)
  {
    return stackListMap.values(key).mapToObj(value -> Value.asObject(value, valueClass));
  }

  protected void replaceAll(long key, long value, long newValue)
  {
    for (int i=0; i<stackListMap.getValueCount(key); i++) if (stackListMap.get(key, i)==value) stackListMap.put(key, i, newValue);
    setModified(true);
  }

  /**
   * Replaces all occurrences of given value with new value
   * @param key the key
   * @param value the old value
   * @param newValue the new value
   */
  public void replaceAll(long key, T value, T newValue)
  {
    replaceAll(key, Value.asLong(value), Value.asLong(newValue));
  }

  protected void push(long key, long value)
  {
    stackListMap.push(key, value);
    setModified(true);
  }

  /**
   * Adds value to the end of the stack
   * @param key the key
   * @param value the value
   */
  public void push(long key, T value)
  {
    push(key, Value.asLong(value));
  }

  protected long popAsLong(long key)
  {
    setModified(true);
    return stackListMap.pop(key);
  }

  /**
   * Pops the value at the end of the stack
   * @param key the key
   * @return the value
   */
  @SuppressWarnings("unchecked")
  public T pop(long key)
  {
    return (T) Value.asObject(popAsLong(key), valueClass);
  }

  /**
   * If a value is in the stack, low-level representation to be used in queries
   * @param key the key
   * @param value the value to search for
   * @return true, if it's in the stack
   */
  public boolean containsValue(long key, long value)
  {
    return stackListMap.indexOf(key, 0, value)>-1;
  }

  /**
   * If a value is in the stack
   * @param key the key
   * @param value the value to search for
   * @return true, if it's in the stack
   */
  public boolean containsValue(long key, T value)
  {
    return stackListMap.indexOf(key, 0, Value.asLong(value))>-1;
  }
}
