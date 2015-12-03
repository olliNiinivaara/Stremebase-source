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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import com.stremebase.dal.Field;
import com.stremebase.dal.Table;
import com.stremebase.dal.Value;
import com.stremebase.map.ArrayMap;

/**
 * A field to store global database variables efficiently
 * <p>
 * Variable names are strings of at most 8 characters, where characters must belong to the ascii 256-range.
 *  
 * @author olli
 *
 */
@SuppressWarnings("rawtypes")
public class VarField extends Field
{
  protected static class VarData
  {
    final long key;
    final long varKey;
    final Class<?> valueClass;
    long value;

    public VarData(long key, long varKey, Class<?> valueClass, long value)
    {
      this.key = key;
      this.varKey = varKey;
      this.valueClass = valueClass;
      this.value = value;
    }
  }

  protected static StringBuilder sb = new StringBuilder();

  public final ArrayMap arrayMap;

  protected Map<String, VarData> nameToVar = new HashMap<>();

  /**
   * Defines the field
   * @param name the name of the field, must not conflict with table names
   */
  @SuppressWarnings("unchecked")
  public VarField(String name)
  {
    super(null, name);
    Table.defaultDb.defineArrayMap(name, 3);
    map =  Table.defaultDb.getMap(name);
    arrayMap = (ArrayMap)map;

    arrayMap.keys().forEach(key ->
    {
      VarData data = new VarData(key, arrayMap.get(key, 0), readClass((int) arrayMap.get(key, 1)), arrayMap.get(key, 2));
      nameToVar.put(varKeyToVarName(data.varKey), data);
    });
  }

  /**
   * Read the variable with this first. A read variable is then managed in an in-memory hashmap. 
   * @param variable the name of the variable
   * @param valueClass the Class of the variable
   * @param defaultValue the value to store if nothing is yet stored
   * @return the value
   */
  @SuppressWarnings("unchecked")
  public <T> T createOrGet(String variable, Class<?> valueClass, T defaultValue)
  {
    VarData data = nameToVar.get(variable);
    if (data!=null) return Value.asObject(data.value, data.valueClass);

    assertValidValueClass(valueClass);
    long key = arrayMap.getLargestKey()+1;
    long varKey = varNameToVarKey(variable);
    data = new VarData(key, varKey, valueClass, Value.asLong(defaultValue));
    nameToVar.put(variable, data);
    arrayMap.put(key, 0, varKey);
    arrayMap.put(key, 1, writeClass(valueClass));
    arrayMap.put(key, 2, Value.asLong(defaultValue));
    arrayMap.flush();
    return defaultValue;
  }

  /**
   * Gets the variable
   * @param variable the name
   * @return the value
   */
  public <T> T get(String variable)
  {
    VarData data = nameToVar.get(variable);
    if (data==null) throw new IllegalArgumentException(variable+ "does not exist");
    return Value.asObject(data.value, data.valueClass);
  }

  /**
   * Sets the variable, flushes instantly to disk
   * @param variable the name
   * @param value the value
   */
  public <T> void set(String variable, T value)
  {
    VarData data = nameToVar.get(variable);
    if (data==null) throw new IllegalArgumentException(variable+ "does not exist");
    data.value = Value.asLong(value);
    arrayMap.put(data.key, 2, Value.asLong(value));
    arrayMap.flush();
  }

  protected long varNameToVarKey(String variable)
  {
    if (variable.length()>8) throw new IllegalArgumentException("variable name length > 8");
    variable = variable.toLowerCase();

    long key = 0;

    for (long i=0; i<variable.length(); i++)
    {
      char c = variable.charAt((int)i);
      if (c>255) throw new IllegalArgumentException("char in variable name is not in ascii 256");
      key += (long)c << 8L*i;
    }

    return key;
  }

  protected String varKeyToVarName(long varKey)
  {
    sb.setLength(0);
    char c = 0;
    int i = 0;

    while (i<8)
    {
      byte b = (byte)varKey;
      c = (char)b;
      if (c==0) break;
      sb.append(c);
      varKey = varKey >> 8;
    i++;
    }

    return sb.toString();
  }

  protected Class<?> readClass(int classCode)
  {
    if (classCode==1) return OptionalLong.class;
    if (classCode==2) return OptionalDouble.class;
    if (classCode==3) return BitSet.class;
    if (classCode==4) return Instant.class;
    if (classCode==5) return LocalTime.class;
    if (classCode==6) return LocalDate.class;
    if (classCode==7) return String.class;
    return null;
  }

  protected int writeClass(Class<?> valueClass)
  {
    if (valueClass.equals(OptionalLong.class)) return 1;
    if (valueClass.equals(OptionalDouble.class)) return 2;
    if (valueClass.equals(BitSet.class)) return 3;
    if (valueClass.equals(Instant.class)) return 4;
    if (valueClass.equals(LocalTime.class)) return 5;
    if (valueClass.equals(LocalDate.class)) return 6;
    if (valueClass.equals(String.class)) return 7;
    return -1;
  }

  /**
   * VarField cannot be indexed.
   */
  @Override
  @Deprecated
  public void addIndex(byte indexType)
  {
    throw new UnsupportedOperationException("Varfields cannot be indexed.");
  }
}
