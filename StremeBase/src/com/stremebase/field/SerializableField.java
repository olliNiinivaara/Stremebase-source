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


import java.io.Serializable;

import com.stremebase.dal.Field;
import com.stremebase.dal.Table;
import com.stremebase.map.ObjectMap;


/**
 * A field to store any serializable value. Naturally can not be indexed.
 * @author olli
 *
 * @param <T> the Class of the value
 */
public class SerializableField<T extends Serializable> extends Field<T>
{
  public final Class<? extends Serializable> valueClass;

  /**
   * Defines new serializable field.
   * @param table the table
   * @param name the name
   * @param valueClass the Class of the value
   */
  public SerializableField(Table table, String name, Class<? extends Serializable> valueClass)
  {
    super(table, name);
    this.valueClass = valueClass;
    table.tableDb.defineMap(table.name+"_"+name, ObjectMap.class);
    map = table.tableDb.getMap(table.name+"_"+name);
  }

  /**
   * gets the low level representation of the object
   * @param key the key
   * @return the object as long array of bytes
   */
  public long[] getAsBytes(long key)
  {
    return ((ObjectMap)map).getAsBytes(key);
  }

  /**
   * Sets the value as long array of bytes
   * @param key the key
   * @param bytes the value
   */
  public void set(long key, long[] bytes)
  {
    ((ObjectMap)map).putBytes(key, bytes);
    setModified(true);
  }

  /**
   * gets the value
   * @param key the key
   * @return the object
   */
  @SuppressWarnings("unchecked")
  public T get(long key)
  {
    return (T) ((ObjectMap)map).get(key);
  }

  /**
   * sets the value
   * @param key the key
   * @param value the object
   */
  public void set(long key, T value)
  {
    ((ObjectMap)map).put(key, value);
    setModified(true);
  }

  /**
   * SerializableField cannot be indexed.
   */
  @Override
  @Deprecated
  public void addIndex(byte indexType)
  {
    throw new UnsupportedOperationException("This field is not indexable.");
  }
}
