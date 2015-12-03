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

package com.stremebase.dal;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import com.stremebase.base.StremeMap;
import com.stremebase.field.SerializableField;
import com.stremebase.map.ObjectMap;


/**
 * The abstract base class for fields.
 * <p>
 * StremeBase fields in general accept following Classes: {@link BitSet} (max 60 bits), {@link OptionalLong}, {@link OptionalDouble}, {@link Instant},
 * {@link LocalTime}, {@link LocalDate}, and {@link String} (for representing one word/term/token)
 * <p>
 * Moreover, {@link com.stremebase.field.SerializableField} stores any serializable value and {@link com.stremebase.field.TextField} stores texts consisting of many words.
 * <p>
 * You can also easily develop new, application-specific fields. 
 * @author olli
 *
 * @param <T> The class of the values stored in this field
 */
public abstract class Field<T>
{
  public StremeMap map;

  public final Table table;
  public final String name;

  public boolean isModified;

  /**
   * If set to true, every write is instantly flushed to disk
   */
  public boolean autoFlush;

  /**
   * Creates a new field for a table. Note that first field created becomes the primary field (which must contain all keys).
   * @param table the table
   * @param name the name
   */
  public Field(Table table, String name)
  {
    this.table = table;
    if (table!=null) table.addField(this);
    this.name = name;
  }

  protected void assertValidValueClass(Class<?> clazz)
  {
    if (clazz == SerializableField.class)
    {
      if (!(Serializable.class.isAssignableFrom(clazz))) throw new UnsupportedOperationException(clazz.getName()+" is not Serializable.");
      return;
    }

    if (!(clazz == OptionalLong.class || clazz == OptionalDouble.class
        || clazz == BitSet.class
        || clazz == Instant.class || clazz == LocalTime.class || clazz == LocalDate.class
        || clazz == String.class)) throw new UnsupportedOperationException("ValueClass must be one of: "+
            "OptionalLong.class, "+
            "OptionalDouble.class, BitSet.class, Instant.class, LocalTime.class, LocalDate.class, "+
            "String.class)");
  }

  /**
   * Adds an index to a field. Index allows faster querying (no nees to linearly access all keys), but as downside writes are slower and memory consumption is higher - use with care
   * @param indexType One the relation types defined in DB
   */
  public void addIndex(byte indexType)
  {
    if (map instanceof ObjectMap) throw new UnsupportedOperationException("This field is not indexable.");
    table.tableDb.defineIndex(table.name+"_"+name, indexType);
  }

  protected void setModified(boolean modified)
  {
    isModified = modified;
    if (isModified && autoFlush) flush();
  }

  /**
   * If isModified, flushes changes to disk
   */
  public void flush()
  {
    if (isModified) map.flush();
    isModified = false;
  }

  /**
   * Returns the next available key
   * <p>
   * The callee should always be the primaryField of a table
   * @return the next key
   */
  public long getFreeKey()
  {
    if (isModified) flush();
    return map.getLargestKey()+1;
  }

  /**
   * Removes key and associated values
   * @param key the key
   */
  public void remove(long key)
  {
    if (key<0) return;
    map.remove(key);
    setModified(true);
  }
}