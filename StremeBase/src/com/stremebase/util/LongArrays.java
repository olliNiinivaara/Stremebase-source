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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

import com.stremebase.base.DB;

/**
 * Utility methods for working with arrays of longs
 * <p>
 * Also contains the LongComparator interface for sorting long arrays on unnatural sort orders 
 * @author olli
 *
 */
public class LongArrays
{
  public static int indexOf(long[] array, long value)
  {
    if (array==null) return -1;
    for (int i=0; i<array.length; i++) if (array[i]==value) return i;
    return -1;
  }

  public static int getDimensionCount(Object array)
  {
    if (array==null) return -1;
    int count = 0;
    Class<?> arrayClass = array.getClass();
    while (arrayClass.isArray())
    {
      count++;
      arrayClass = arrayClass.getComponentType();
    }
    return count;
  }

  public static long[] subArray(long[] fromArray, int from, int length)
  {
    long[] result = new long[length-from];
    System.arraycopy(fromArray, from, result, 0, length);
    return result;
  }

  public static long[] subArray(long[] fromArray, int length)
  {
    long[] result = new long[length];
    System.arraycopy(fromArray, 0, result, 0, length);
    return result;
  }

  public static long[] union(long[] array1, long[] array2)
  {
    return LongStream.concat(Arrays.stream(array1), Arrays.stream(array2)).distinct().toArray();
  }

  public static int[] indexArray(int length)
  {
    int[] result = new int[length];
    for (int i=0; i<length; i++) result[i] = i;
    return result;
  }

  public static long[] nullArray(int length)
  {
    long[] result = new long[length];
    for (int i=0; i<length; i++) result[i] = DB.NULL;
    return result;
  }

  public static boolean contains(long[] array, long value)
  {
    if (array==null || array.length==0) return false;
    for (long l: array) if (l==value) return true;
    return false;
  }

  final static Random rnd = ThreadLocalRandom.current();

  public static void shuffle(long[] array, int fromIndex, int toIndex)
  {
    a = array;
    for (int i = toIndex; i > fromIndex; i--) exch(rnd.nextInt(i + 1), i);
  }

  /**
   * Enables sorting long arrays to unnatural orders
   * @author olli
   *
   */
  @FunctionalInterface
  public static interface LongComparator
  {
    long compare(long v, long w);
  }

  static LongComparator less;
  static long[] a;

  public static void sort(long[] array, LongComparator comparator)
  {
    sort(array, comparator, 0, array.length-1);
  }

  public static void sort(long[] array, LongComparator comparator, int fromIndex, int toIndex)
  {
    if (toIndex-fromIndex<1) return;
    less = comparator;
    a = array;
    if (toIndex-fromIndex==1)
    {
      sort2(fromIndex);
      return;
    };

    shuffle(array, fromIndex, toIndex);
    sort(fromIndex, toIndex);
  }

  protected static void sort2(int fromIndex)
  {
    if (less.compare(a[fromIndex+1], a[fromIndex])<0) exch(fromIndex, fromIndex+1);
  }

  //Robert Sedgewick...
  protected static void sort(int lo, int hi)
  {
    if (hi <= lo) return;

    if (less.compare(a[hi], a[lo])<0) exch(lo, hi);

    int lt = lo + 1, gt = hi - 1;
    int i = lo + 1;

    while (i <= gt)
      if       (less.compare(a[i], a[lo])<0) exch(lt++, i++);
      else if  (less.compare(a[hi], a[i])<0) exch(i, gt--);
      else                         i++;
    exch(lo, --lt);
    exch(hi, ++gt);

    sort(lo, lt-1);
    if (less.compare(a[lt], a[gt])<0) sort(lt+1, gt-1);
    sort(gt+1, hi);
  }

  private static void exch(int i, int j)
  {
    long swap = a[i];
    a[i] = a[j];
    a[j] = swap;
  }

  public static long[] merge(long[] a, long[] b, LongComparator comparator)
  {
    long[] result = new long[a.length + b.length];
    int i = a.length - 1;
    int j = b.length - 1;
    int k = result.length;

    while (k > 0)
      result[--k] = (j < 0 || (i >= 0 && comparator.compare(a[i], b[j])>=0)) ? a[i--] : b[j--];

      return result;
  }

  public static long[] merge(int resultLength, long[] a, int aFromIndex, int aToIndex, long[] b, int bFromIndex, int bToIndex, LongComparator comparator)
  {
    long[] result = new long[resultLength];
    int i = aToIndex;
    int j = bToIndex;
    int k = resultLength;

    while (k > 0)
      result[--k] = (j < bFromIndex || (i >= aFromIndex && comparator.compare(a[i], b[j])>=0)) ? a[i--] : b[j--];

      return result;
  }

  public static long[] negate(long[] array)
  {
    for (int i=0; i<array.length; i++) array[i] = -array[i];
    return array;
  }
}
