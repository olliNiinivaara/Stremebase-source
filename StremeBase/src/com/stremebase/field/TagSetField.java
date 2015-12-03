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
 * A field to store a set where a tag can be associated with a value.
 * @author olli
 *
 * @param <T> the Class of the values
 * @param <TT> the Class of the tag
 */
public class TagSetField<T, TT> extends Field<T>
{
  /**
   * High level representation of the value and it's assoicated tag
   * @author olli
   *
   * @param <T> the Class of the value
   * @param <TT> the Class of the tag
   */
  public static class TagSetEntry<T, TT>
  {
    long key;
    T value;
    TT tag;

    public TagSetEntry(long key, T value, TT tag)
    {
      this.key = key;
      this.value = value;
      this.tag = tag;
    }
  }

  public final SetMap setMap;
  public final Class<?> valueClass;
  public final Class<?> tagValueClass;

  /**
   * Defines the field
   * @param table the table
   * @param name the name
   * @param valueClass the Class of the value
   * @param tagValueClass the Class of the tag
   */
  public TagSetField(Table table, String name, Class<?> valueClass, Class<?> tagValueClass)
  {
    super(table, name);
    assertValidValueClass(valueClass);
    assertValidValueClass(tagValueClass);
    this.valueClass = valueClass;
    this.tagValueClass = tagValueClass;
    table.tableDb.defineAttributedSetMap(table.name+"_"+name);
    map = table.tableDb.getMap(table.name+"_"+name);
    setMap = (SetMap)map;
  }

  /**
   * Gets the values and the tags associated with a key as a stream
   * @param key the key
   * @return stream of values
   */
  @SuppressWarnings("unchecked")
  public Stream<TagSetEntry<T, TT>> getAsStream(long key)
  {
    return setMap.entries(key).map(e -> new TagSetEntry<T, TT>(e.key, (T) Value.asObject(e.value, valueClass), (TT) Value.asObject(e.attribute, tagValueClass)));
  }

  protected void add(long key, long... values)
  {
    for (long value: values) setMap.put(key, value);
    setModified(true);
  }

  /**
   * Adds values without defining tags
   * @param key the key
   * @param values the values
   */
  public void add(long key, @SuppressWarnings("unchecked") T... values)
  {
    add(key, Value.asLongArray(values));
  }

  /**
   * Gets the low-level representation of the values and the attributes, to be used in queries
   * @param key the key
   * @return stream of set entries
   */
  public Stream<SetEntry> entries(long key)
  {
    return setMap.entries(key);
  }

  protected void add(long key, long value, long tag)
  {
    setMap.put(key, value, tag);
    setModified(true);
  }

  /**
   * Adds a value and it's tag to a set
   * @param key the set key
   * @param value the value
   * @param tag the tag
   */
  public void add(long key, T value, TT tag)
  {
    setMap.put(key, Value.asLong(value), Value.asLong(tag));
    setModified(true);
  }

  /**
   * Adds multiple entries to field
   * @param entries the entries to be added
   */
  public void add(@SuppressWarnings("unchecked") TagSetEntry<T, TT>... entries)
  {
    for (TagSetEntry<T, TT> e: entries) add(e.key, Value.asLong(e.value), Value.asLong(e.tag));
  }
}