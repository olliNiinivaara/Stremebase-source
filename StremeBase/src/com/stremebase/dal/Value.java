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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import com.stremebase.base.DB;
import com.stremebase.base.To;

/**
 * Highest level methods for reading and writing data to database.
 * @author olli
 *
 */
public class Value
{
  public static To to;

  /**
   * Converts the object to the low-level database format. Use this in filters to avoid converting queried data to objects.
   * @param value the object
   * @return the long that represents the object
   */
  public static <T> long asLong(T value)
  {
    if (value==null) return DB.NULL;

    if (value instanceof BitSet) return To.data((BitSet)value);
    if (value instanceof OptionalLong) return To.data((OptionalLong)value);
    if (value instanceof OptionalDouble) return To.data((OptionalDouble)value);
    if (value instanceof Instant) return To.data((Instant)value);
    if (value instanceof LocalTime) return To.data((LocalTime)value);
    if (value instanceof LocalDate) return To.data((LocalDate)value);
    if (value instanceof String) return to.data((String)value);

    if (value instanceof Integer) return Long.valueOf((int)value);

    return (long)value;
  }

  /**
   * Does not generate the string to lexicon if it's not there. Use this with queries.
   * @param value the string
   * @return DB.NULL, if the string is not used, otherwise the long
   */
  public static long asLongIfExists(String value)
  {
    if (value==null) return DB.NULL;
    return to.dataIfExists(value);
  }

  /**
   * Converts the objects to an array of the low-level database format. Use this in filters to avoid converting queried data to objects.
   * @param values the objects
   * @return the array
   **/
  @SafeVarargs
  public static <T> long[] asLongArray(T... values)
  {
    if (values==null || values.length==0) return null;
    long[] result = new long[values.length];
    for (int i=0; i<result.length; i++) result[i] = asLong(values[i]);
    return result;
  }

  /**
   * Does not generate the string to lexicon if it's not there. Use this with queries.
   * @param values the strings
   * @return the long array with DB.NULL everywhere where the string does not exist 
   */
  public static long[] asLongArrayIfExists(String... values)
  {
    if (values==null || values.length==0) return null;
    long[] result = new long[values.length];
    for (int i=0; i<result.length; i++) result[i] = to.dataIfExists(values[i]);
    if (result.length==1 && result[0]==DB.NULL) return null;
    return result;
  }

  /**
   * Converts the low-level representation back to object
   * @param l the long
   * @param clazz the Class of the object
   * @return the object
   */
  @SuppressWarnings("unchecked")
  public static <T> T asObject(long l, Class<?> clazz)
  {
    if (l==DB.NULL) return null;

    if (clazz.equals(BitSet.class)) return (T) To.bitSet(l);
    if (clazz.equals(OptionalDouble.class)) return (T) To.optionalDouble(l);
    if (clazz.equals(OptionalLong.class)) return (T) To.optionalLong(l);
    if (clazz.equals(Instant.class)) return (T) To.instant(l);
    if (clazz.equals(LocalTime.class)) return (T) To.localTime(l);
    if (clazz.equals(LocalDate.class)) return (T) To.localDate(l);
    if (clazz.equals(String.class)) return (T) to.string(l);

    return null;
  }

  /**
   * Converts the low-level representation back to object wrapped into an Optional
   * @param l the long
   * @param clazz the Class of the object
   * @return the Optional
   */
  @SuppressWarnings("unchecked")
  public static <T> Optional<T> asOptional(long l, Class<?> clazz)
  {
    if (l==DB.NULL) return Optional.empty();
    if (clazz.equals(OptionalDouble.class) || clazz.equals(OptionalLong.class)) throw new IllegalArgumentException("get "+clazz.getName()+" asObject()");
    if (clazz.equals(BitSet.class)) return (Optional<T>) Optional.of(To.bitSet(l));
    if (clazz.equals(Instant.class)) return (Optional<T>) Optional.of(To.instant(l));
    if (clazz.equals(LocalTime.class)) return (Optional<T>) Optional.of(To.localTime(l));
    if (clazz.equals(LocalDate.class)) return (Optional<T>) Optional.of(To.localDate(l));
    if (clazz.equals(String.class)) return (Optional<T>) Optional.of(to.string(l));

    return null;
  }
}
