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

package com.stremebase.base;

import java.time.Instant;
import java.time.LocalDateTime;

import com.stremebase.base.util.Lexicon;


/**
 * A collection of static methods for converting data to longs and back
 */
public abstract class To
{
  /**
   * Converts a double to long
   * @param d the double
   * @return as long
   */
  public static long l(double d)
  {
    return d==DB.NULL ? DB.NULL : sortableDoubleBits(Double.doubleToLongBits(d));
  }

  /**
   * Converts a long back to double
   * @param l the long
   * @return as double
   */
  public static double toDouble(long l)
  {
    return l==DB.NULL ? DB.NULL : Double.longBitsToDouble(sortableDoubleBits(l));
  }

  //Function stolen from org.apache.lucene.util.NumericUtils (thanks, Apache Software Foundation)
  protected static long sortableDoubleBits(long bits)
  {
    return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
  }

  /**
   * Converts an {@link Instant} to long
   * @param i the instant
   * @return as long
   */
  public static long l(Instant i)
  {
    return i==null ? DB.NULL : i.toEpochMilli();
  }

  /**
   * Converts a long back to {@link Instant}
   * @param l the long
   * @return as {@link Instant}
   */
  public static Instant instant(long l)
  {
    return l==DB.NULL ? null : Instant.ofEpochMilli(l);
  }

  /**
   * Converts an {@link LocalDateTime} to long
   * Uses {@link com.stremebase.base.DB#ZONE}
   * @param ldt the localdatetime
   * @return as long
   */
  public static long l(LocalDateTime ldt)
  {
    return ldt==null ? DB.NULL : ldt.atZone(DB.db.ZONE).toInstant().toEpochMilli();
  }

  /**
   * Converts a long back to {@link LocalDateTime}
   * Uses {@link com.stremebase.base.DB#ZONE}
   * @param l the long
   * @return as {@link LocalDateTime}
   */
  public static LocalDateTime localDateTime(long l)
  {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(l), DB.db.ZONE);
  }

  /**
   * Stores a boolean to a long at given index
   * @param bool the boolean to store
   * @param index the index to use (use 0-62)
   * @param thelong the existing long to modify
   * @return the long with the bit set accordingly
   */
  public static long l(boolean bool, long index, long thelong)
  {
    return thelong |= ((bool ? 1l : 0l) << index);
  }

  /**
   * Reads back a boolean from a long at given index
   * @param l the long
   * @param index the index
   * @return the bit at given index as boolean
   */
  public static boolean toBoolean(long l, long index)
  {
    return ((l >> index) & 1l) == 1l ? true : false;
  }

  /**
   * Creates a mask for querying longs that store boolean arrays
   * <p>
   * Example:
   * final long mask = To.mask(2, 3); //query for indices 2 and 3  
   * map.keyset().filter(key -&gt; ((map.get(key) &amp; mask) == mask)).forEach(key -&gt; booleans 2 and 3 are true);
   * map.keyset().filter(key -&gt; ((map.get(key) &amp; mask) == 0)).forEach(key -&gt; booleans 2 and 3 are false);   * <p>
   * @param indexes the relevant indices
   * @return the mask
   */
  public static long mask(long... indexes)
  {
    long mask = 0;
    for (long pos: indexes) mask = (mask | (1l << pos));
    return mask;
  }

  /**
   * Converts a {@link CharSequence} to long
   * @param word the term to convert
   * @return as long
   */
  public static long l(CharSequence word)
  {
    return Lexicon.useWord(word, true);
  }

  /**
   * Converts a long back to a string
   * @param l the long
   * @param string the (empty) {@link StringBuilder} where to put the string 
   */
  public static void toString(long l, StringBuilder string)
  {
    Lexicon.getWord(l, string);
  }

  /**
   * A helper method for getting back a string from a long
   * Slow - creates 2 new objects.
   * @param l the long
   * @return the word identified by the long
   */
  public static String toString(long l)
  {
    StringBuilder string = new StringBuilder();
    Lexicon.getWord(l, string);
    return string.toString();
  }
}
