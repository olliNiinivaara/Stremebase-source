package com.stremebase.base;

import java.time.Instant;
import java.time.LocalDateTime;

import com.stremebase.base.util.Lexicon;


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
  
  //Function stolen from org.apache.lucene.util.NumericUtils (thanks, Apache Software Foundation)
  protected static long sortableDoubleBits(long bits)
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
  
  public static long l(CharSequence word)
  {
    return Lexicon.useWord(word, true);
  }
    
  public static void toString(long l, StringBuilder string)
  {
    Lexicon.getWord(l, string);
  } 
  
  public static String toString(long l)
  {
    StringBuilder string = new StringBuilder();
    Lexicon.getWord(l, string);
    return string.toString();
  } 
}
