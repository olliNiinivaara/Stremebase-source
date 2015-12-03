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

package com.stremebase.util;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.stremebase.base.DB;
import com.stremebase.util.LongArrays.LongComparator;


/**
 * Array that grows as needed without copying of existing values
 * @author olli
 *
 */
public class ExpandingArray
{
  private long[][] array = new long[65536][];
  public int length = 0;


  public ExpandingArray()
  {
  }

  public ExpandingArray(int firstFree)
  {
    length = firstFree;
  }

  public void clear()
  {
    length = 0;
  }

  public void add(long i)
  {
    set(length, i);
  }

  public void set(int position, long i)
  {
    if (array[position >>> 16]==null) array[position >>> 16] = new long[65536];
    array[position >>> 16][position & 0x0000FFFF] = i;
    if (position>=length) length = position+1;
  }

  public long get(int position)
  {
    if (array[position >>> 16]==null) return DB.NULL;
    return array[position >>> 16][position & 0x0000FFFF];
  }

  public long[] toArray()
  {
    long[] longArray = new long[length];
    if (length==0) return longArray;

    int arrays = (length-1) >>> 16;
    for (int i=0; i<arrays; i++) System.arraycopy(array[i], 0, longArray, i*65536, 65536);
    System.arraycopy(array[arrays], 0, longArray, arrays*65536, length & 0x0000FFFF);
    return longArray;
  }

  public long[] toArray(int newLength)
  {
    long[] longArray = new long[newLength];
    if (newLength==0) return longArray;
    if (newLength>length) newLength = length;

    int arrays = (newLength-1) >>> 16;
    for (int i=0; i<arrays; i++) System.arraycopy(array[i], 0, longArray, i*65536, 65536);
    System.arraycopy(array[arrays], 0, longArray, arrays*65536, newLength & 0x0000FFFF);
    return longArray;
  }

  final static Random rnd = ThreadLocalRandom.current();

  public void shuffle()
  {
    for (int i = length - 1; i > 0; i--)
    {
      int index = rnd.nextInt(i + 1);
      long a = get(index);
      set(index, get(i));
      set(i, a);
    }
  }

  LongComparator less;

  public void sort(LongComparator comparator)
  {
    less = comparator;
    shuffle();
    sort(0, length-1);
  }

  //Robert Sedgewick...
  protected void sort(int lo, int hi)
  {
    if (hi <= lo) return;

    if (less.compare(get(hi), get(lo))<0) exch(lo, hi);

    int lt = lo + 1, gt = hi - 1;
    int i = lo + 1;

    while (i <= gt)
      if       (less.compare(get(i), get(lo))<0) exch(lt++, i++);
      else if  (less.compare(get(hi), get(i))<0) exch(i, gt--);
      else                         i++;
    exch(lo, --lt);
    exch(hi, ++gt);

    sort(lo, lt-1);
    if (less.compare(get(lt), get(gt))<0) sort(lt+1, gt-1);
    sort(gt+1, hi);
  }

  protected void exch(int i, int j)
  {
    long swap = get(i);
    set(i, get(j));
    set(j, swap);
  }
}