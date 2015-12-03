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

import java.util.BitSet;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.LongStream;

import com.stremebase.base.DB;
import com.stremebase.dal.Field;
import com.stremebase.dal.Table;
import com.stremebase.dal.Value;
import com.stremebase.map.ArrayMap;


/**
 * The field to store a tuple of values
 * @author olli
 *
 * @param <T> The common superClass of all stores classes, such as Object
 */
public class TupleField<T> extends Field<T>
{
  public final ArrayMap arrayMap;

  protected Class<?>[] valueClasses;

  /**
   * defines the field
   * @param table the table
   * @param name the name
   * @param valueClasses the Classes of the values
   */
  public TupleField(Table table, String name, Class<?>... valueClasses)
  {
    super(table, name);
    this.valueClasses = valueClasses;
    for (Class<?> clazz: valueClasses) assertValidValueClass(clazz);
    table.tableDb.defineArrayMap(table.name+"_"+name, valueClasses.length);
    map = table.tableDb.getMap(table.name+"_"+name);
    arrayMap = (ArrayMap)map;
  }

  /**
   * Defines an index to a particular cell of the tuple
   * @param cellIndex the cell index of the value in the tuple
   * @param indexType see db constants for possible values
   */
  public void addIndex(int cellIndex, byte indexType)
  {
    if (valueClasses[cellIndex] == BitSet.class) throw new IllegalArgumentException("Booleans cannot be indexed - filter with mask");
    arrayMap.addIndextoCell(table.tableDb, indexType, cellIndex);
  }

  /**
   * Gets the low-level representation of the value, to be used in queries
   * @param key the key
   * @param index the index of the value
   * @return the value
   */
  public long getAsLong(long key, int index)
  {
    return arrayMap.get(key, index);
  }

  /**
   * Gets the value
   * @param key the key
   * @param index the index
   * @return the value
   */
  @SuppressWarnings("unchecked")
  public <X> X get(long key, int index)
  {
    return (X) Value.asObject(getAsLong(key, index), valueClasses[index]);
  }

  /**
   * Gets the value as {@link Optional}
   * @param key the key
   * @param index the index
   * @return the value
   */
  public <X> Optional<X> getAsOptional(long key, int index)
  {
    return Value.asOptional(getAsLong(key, index), valueClasses[index]);
  }

  /**
   * Gets the low-level representations of all values as an array.
   * @param key the key
   * @return the array of values
   */
  public long[] getAsLongArray(long key)
  {
    return arrayMap.get(key);
  }

  protected void set(long key, int index, long... values)
  {
    if (index+values.length>arrayMap.getValueCount(-1)) throw new IllegalArgumentException("Values from index don't fit into array");
    if (index==0) arrayMap.put(key, values);
    else for (int i=0; i<values.length; i++) arrayMap.put(key, index+i, values[i]);
    setModified(true);
  }

  /**
   * Sets a value
   * @param key the key
   * @param index the index
   * @param value the value
   */
  public void set(long key, int index, Object value)
  {
    arrayMap.put(key, index, Value.asLong(value));
    setModified(true);
  }

  /**
   * Sets the value if it is not yet set.
   * @param key the key
   * @param index the index
   * @param value the value
   * @return the key that is associated with the value. 
   */
  public long setOrGet(long key, int index, Object value)
  {
    if (value==null) return DB.NULL;
    long lvalue = Value.asLong(value);
    OptionalLong existing = arrayMap.queryByCell(index, lvalue, lvalue).findAny();
    if (existing.isPresent()) return existing.getAsLong();
    arrayMap.put(key, index, lvalue);
    setModified(true);
    return key;
  }

  /**
   * Generates new key and sets the value, but only if the value is not associated with any existing key
   * @param index the index
   * @param value the value
   * @return the key that is associated with the value so that existing key is returned as negative value and created key as positive value
   */
  public long createOrGet(int index, Object value)
  {
    if (value==null) return DB.NULL;

    long lvalue = Value.asLong(value);
    if (lvalue==DB.NULL) return DB.NULL;

    OptionalLong existing = arrayMap.queryByCell(index, lvalue, lvalue).findAny();
    if (existing.isPresent()) return -existing.getAsLong();

    long key = getFreeKey();
    arrayMap.put(key, index, lvalue);
    setModified(true);
    return key;
  }

  protected void setAsLongArray(long key, long... values)
  {
    arrayMap.put(key, values);
    setModified(true);
  }

  /**
   * Sets multiple values for a key at once
   * @param key the key
   * @param values the values, starting from index 0
   */
  public void set(long key, @SuppressWarnings("unchecked") T... values)
  {
    setAsLongArray(key, Value.asLongArray(values));
  }

  /**
   * A helper function to generate a result stream for table query from index
   * @param cellIndex index of the value
   * @param lowestValue lowest value to accept
   * @param highestValue highest value to accept
   * @return the stream of matching keys
   */
  public LongStream queryWithIndex(int cellIndex, T lowestValue, T highestValue)
  {
    if (!arrayMap.isCellIndexed(cellIndex)) throw new IllegalArgumentException(cellIndex+" cell is not indexed");
    return arrayMap.queryByCell(cellIndex, Value.asLong(lowestValue), Value.asLong(highestValue));
  }
}
