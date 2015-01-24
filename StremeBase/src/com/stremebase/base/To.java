package com.stremebase.base;

import java.time.Instant;
import java.time.LocalDateTime;


public abstract class To
{  
  public static long l(double d)
  {
    return d==DB.NULL ? DB.NULL : sortableDoubleBits(Double.doubleToLongBits(d));
  }
  
  public static double toDouble(long l)
  {
    return l==DB.NULL ? DB.NULL : Double.longBitsToDouble(sortableDoubleBits(l));
  }
  
  //Converts IEEE 754 representation of a double to sortable order (or back to the original)
  //Function stolen from org.apache.lucene.util.NumericUtils
  //Thanks, Apache Software Foundation
  public static long sortableDoubleBits(long bits)
  {
    return bits ^ (bits >> 63) & 0x7fffffffffffffffL;
  }
  
  public static long l(Instant i)
  {
    return i==null ? DB.NULL : i.toEpochMilli();
  }
  
  public static Instant instant(long l)
  {
    return l==DB.NULL ? null : Instant.ofEpochMilli(l);
  }
  
  public static long l(LocalDateTime ldt)
  {
    return ldt==null ? DB.NULL : ldt.atZone(DB.db.ZONE).toInstant().toEpochMilli();
  }
  
  public static LocalDateTime localDateTime(long l)
  {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(l), DB.db.ZONE);
  }

  public static long l(boolean b, long i, long l)
  {
    return l |= ((b ? 1l : 0l) << i);
  }

  public static boolean toBoolean(long l, long i)
  {
    return ((l >> i) & 1l) == 1l ? true : false;
  }
  
  public static long mask(long... bitPositions)
  {
    long mask = 0;
    for (long pos: bitPositions) mask = (mask | (1l << pos));
    return mask;
  } 
}
