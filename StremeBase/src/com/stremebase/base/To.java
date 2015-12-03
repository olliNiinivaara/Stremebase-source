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
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.OptionalDouble;
import java.util.OptionalLong;


/**
 * A collection of methods for converting data to longs and back.
 * <p>
 * High-level access via {@link com.stremebase.dal.Value}
 * @author olli
 */
public class To
{
  public boolean lowerCaseAll = true;

  protected final DB db;
  protected final StringCache stringCache;

  private final StringBuilder sb = new StringBuilder();
  private final StringBuilder sb2 = new StringBuilder();

  public To(DB db, int cacheSize)
  {
    this.db = db;
    stringCache = cacheSize == 0 ? null : new StringCache(db, cacheSize);
  }

  public To(DB db)
  {
    this.db = db;
    stringCache = new StringCache(db, 20000);
  }

  public static long data(long d)
  {
    return d;
  }

  public static long data(int i)
  {
    return i;
  }

  public static long data(byte b)
  {
    return b;
  }

  public static long data(Boolean b)
  {
    if (b==null) return DB.NULL;
    return b ? 1 : 0;
  }

  public static boolean toBoolean(long data)
  {
    return data>0 ? true : false;
  }

  public static long data(OptionalLong l)
  {
    if (l==null) return DB.NULL;
    if (!l.isPresent()) return DB.NULL;
    return l.getAsLong();
  }

  public static OptionalLong optionalLong(long data)
  {
    if (data==DB.NULL) return OptionalLong.empty();
    return OptionalLong.of(data);
  }

  public static long data(double d)
  {
    return d==DB.NULL ? DB.NULL : sortableDoubleBits(Double.doubleToLongBits(d));
  }

  public static long data(OptionalDouble d)
  {
    if (d==null) return DB.NULL;
    if (!d.isPresent()) return DB.NULL;
    return sortableDoubleBits(Double.doubleToLongBits(d.getAsDouble()));
  }

  public static double toDouble(long data)
  {
    return data==DB.NULL ? DB.NULL : Double.longBitsToDouble(sortableDoubleBits(data));
  }

  public static OptionalDouble optionalDouble(long data)
  {
    if (data==DB.NULL) return OptionalDouble.empty();
    return OptionalDouble.of(Double.longBitsToDouble(sortableDoubleBits(data)));
  }

  //from org.apache.lucene.util.NumericUtils...
  protected static long sortableDoubleBits(long bits)
  {
    return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
  }

  public static long data(Instant i)
  {
    return i==null ? DB.NULL : i.toEpochMilli();
  }

  public static Instant instant(long data)
  {
    return data==DB.NULL ? null : Instant.ofEpochMilli(data);
  }

  public static long data(LocalTime ld)
  {
    return ld==null ? DB.NULL : ld.toSecondOfDay();
  }

  public static LocalDate localDate(long data)
  {
    return data==DB.NULL ? null :  LocalDate.ofEpochDay(data);
  }

  public static long data(LocalDate ld)
  {
    return ld==null ? DB.NULL : ld.toEpochDay();
  }

  public static LocalTime localTime(long data)
  {
    return data==DB.NULL ? null : LocalTime.ofSecondOfDay(data);
  }

  public static long data(BitSet bitSet)
  {
    if (bitSet.length()>60) throw new IllegalArgumentException("A BitSet in Stremebase cannot contain more than bits 0-60");
    return bitSet == null ? DB.NULL : bitSet.toLongArray()[0];
  }

  public static BitSet bitSet(long data)
  {
    return data==DB.NULL ? null : BitSet.valueOf(new long[] {data});
  }

  public long data(String word)
  {
    if (word == null) return DB.NULL;
    if (word.isEmpty()) return 0;

    if (lowerCaseAll) word = word.toLowerCase();

    if (stringCache==null) return db.lexicon.useWord(word, true);

    long cached = DB.NULL;
    cached = stringCache.data(word);
    return cached;
  }

  public long dataIfExists(String word)
  {
    if (word == null) return DB.NULL;
    if (word.isEmpty()) return 0;

    if (lowerCaseAll) word = word.toLowerCase();

    if (stringCache==null) return db.lexicon.useWord(word, false);

    long cached = DB.NULL;
    cached = stringCache.dataIfExists(word);
    return cached;
  }

  public String string(long data)
  {
    if (data == DB.NULL) return null;
    if (data==0) return "";

    if (stringCache!=null) return stringCache.toString(data);

    sb.setLength(0);
    db.lexicon.getWord(data, sb);
    return sb.toString();
  }

  public String string(long data, boolean capitalize)
  {
    if (data == DB.NULL) return null;
    if (data==0) return "";

    String result;

    if (stringCache!=null) result = stringCache.toString(data);
    else
    {
      sb.setLength(0);
      db.lexicon.getWord(data, sb);
      result = sb.toString();
    }

    if (!capitalize || result.length()==0) return result;

    sb2.setLength(0);
    sb2.append(Character.toUpperCase(result.charAt(0)));
    if (result.length()>1) sb2.append(result.substring(1));
    return sb2.toString();
  }

  protected static class StringCache
  {
    protected final DB db;
    protected final long[] cacheKeys;
    protected final String[] cache;

    private final StringBuilder sb = new StringBuilder();

    public StringCache(DB db, int cacheSize)
    {
      this.db = db;
      cacheKeys = new long[cacheSize];
      cache = new String[cacheSize];
    }

    public String toString(long l)
    {
      if (l<1) return "";

      if (l <= Character.MAX_VALUE) return ""+(char)l;

      sb.setLength(0);
      db.lexicon.getWord(l, sb);

      int cp = (int) l % cacheKeys.length;
      if (cacheKeys[cp] == l) return cache[cp];

      sb.setLength(0);
      db.lexicon.getWord(l, sb);

      cacheKeys[cp] = l;
      cache[cp] = sb.toString();

      return cache[cp];
    }

    public long data(String string)
    {
      return data(string, true);
    }

    public long dataIfExists(String string)
    {
      return data(string, false);
    }

    protected long data(String string, boolean createIfAbsent)
    {
      if (string == null || string.isEmpty()) return 0;

      if (string.length()==1) return string.charAt(0);

      long l = db.lexicon.useWord(string, createIfAbsent);

      if (l!=DB.NULL)
      {
        int cp = (int) l % cacheKeys.length;
        cacheKeys[cp] = l;
        cache[cp] = string;
      }

      return l;
    }
  }
}
