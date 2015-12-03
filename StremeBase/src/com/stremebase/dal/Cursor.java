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

import java.util.Arrays;
import java.util.Collection;
import java.util.PrimitiveIterator;
import java.util.function.LongPredicate;
import java.util.stream.LongStream;

import com.stremebase.base.DB;
import com.stremebase.util.ExpandingArray;
import com.stremebase.util.LongArrays;
import com.stremebase.util.SortOrder;

/**
 * Cursor is the result of a database query
 * @author olli
 *
 */
public interface Cursor
{
  /**
   * Sets whether next() retrieves previous (towards first) or following (towards last) key
   * @param forward the direction
   */
  public void setDirection(boolean forward);

  /**
   * Moves the cursor to first result
   * @return false, if there are no query results
   */
  public boolean first();

  /**
   * Moves the cursor to last result
   * @return false, if there are no query results
   */
  public boolean last();

  /**
   * Moves the cursor to next result, which is depending on direction either previous or following result
   * @return false, if there is no next result
   */
  public boolean next();

  /**
   * Moves the cursor to given position
   * @param position the position
   * @param changeDirection if true, direction is changed if move is towards new direction
   */
  public void moveTo(int position, boolean changeDirection);

  /**
   * The count of search results
   * @return the number
   */
  public int count();

  /**
   * Gets the search result at the current cursor position
   * @return the current result
   */
  public long get();

  /**
   * Gets the search result at the given position (without moving cursor)
   * @param position the position
   * @return the search result
   */
  public long get(int position);

  /**
   * Sorts the search result
   * @param sortOrder the sort criteria
   */
  public void sort(SortOrder sortOrder);


  /**
   * The search result as a stream. Direction is always forwards. 
   * @return the result
   */
  public LongStream getResult();


  static final ExpandingArray buffer = new ExpandingArray();

  /**
   * Returns the right kind of cursor for a search result
   * @param bigData If true, the whole search result is not read into main memory
   * @param stream The search result stream
   * @param filters How the stream is filtered
   * @param sortOrder How the stream is sorted
   * @param limit Maximum number of search results to retrieve. Note that bigData must be limited to Top N if it is to be sorted.
   * @return the appropriate cursor
   */
  public static Cursor getCursor(boolean bigData, LongStream stream, Collection<LongPredicate> filters, SortOrder sortOrder, int limit)
  {
    if (!bigData) return new BasicCursor(stream, filters, sortOrder, limit);
    if (sortOrder!=null && limit>2 && limit < Integer.MAX_VALUE) return new TopNCursor(stream, filters, sortOrder, limit);
    else if (sortOrder==null) return new StreamCursor(stream, filters, limit);
    else throw new IllegalArgumentException("Unlimited stream cannot be sorted.");
  }

  /**
   * The normal not-bigData -cursor, used when search result fits into main memory
   * @author olli
   *
   */
  public static class BasicCursor implements Cursor
  {
    public boolean forward = true;

    protected long[] result;
    protected int index = 0;


    public BasicCursor(LongStream stream, Collection<LongPredicate> filters, SortOrder sortOrder, int limit)
    {
      buffer.clear();
      LongStream resultStream;
      if (filters == null || filters.isEmpty())
      {
        if (limit < 1 || limit == Integer.MAX_VALUE) resultStream = stream;
        else resultStream =stream.limit(limit);
      }
      else
      {
        if (limit < 1 || limit == Integer.MAX_VALUE)
          resultStream = stream.unordered().parallel().filter(filters.stream().reduce(LongPredicate::and).orElse(t->true));
        else resultStream = stream.unordered().parallel().filter(filters.stream().reduce(LongPredicate::and).orElse(t->true)).limit(limit);
      }

      resultStream.forEach(number -> buffer.add(number));
      if (sortOrder!=null) buffer.sort(sortOrder);
      result = buffer.toArray();
    }

    public BasicCursor(long[] result)
    {
      this.result = result;
    }

    @Override
    public boolean first()
    {
      if (result.length==0) return false;
      index = forward ? 0 : result.length-1;
      return true;
    }

    @Override
    public boolean last()
    {
      if (result.length==0) return false;
      index = !forward ? 0 : result.length-1;
      return true;
    }

    @Override
    public boolean next()
    {
      if (forward && index>=result.length-1) return false;
      if (!forward && index<=0) return false;
      index+= forward ? 1 : -1;
      return true;
    }

    @Override
    public void moveTo(int position, boolean changeDirection)
    {
      if (position == index) return;

      if (changeDirection) forward = position>index;

      index = position;
    }

    @Override
    public int count()
    {
      return result.length;
    }

    @Override
    public long get()
    {
      if (index < 0 || index >= result.length) return DB.NULL;
      return result[index];
    }

    @Override
    public long get(int position)
    {
      if (position < 0 || position >= result.length) return DB.NULL;
      return result[position];
    }

    @Override
    public void sort(SortOrder sortOrder)
    {
      if (sortOrder != null) LongArrays.sort(result, sortOrder);
    }

    @Override
    public void setDirection(boolean forward)
    {
      this.forward = forward;
    }

    /**
     * The search result as a stream. Direction is always forwards. 
     * @return the result
     */
    @Override
    public LongStream getResult()
    {
      return Arrays.stream(result);
    }
  }

  /**
   * The big data cursor that will not store search results and therefore allows only forward iteration and does not support sorting
   * @author olli
   *
   */
  public static class StreamCursor implements Cursor
  {
    protected LongStream result;
    protected PrimitiveIterator.OfLong resultIerator;


    public StreamCursor(LongStream stream, Collection<LongPredicate> filters, long limit)
    {
      LongStream resultStream;
      if (filters == null || filters.isEmpty())
      {
        if (limit < 1 || limit == Long.MAX_VALUE) resultStream = stream;
        resultStream =stream.limit(limit);
      }
      else if (limit < 1 || limit == Long.MAX_VALUE)
        resultStream = stream.unordered().filter(filters.stream().reduce(LongPredicate::and).orElse(t->true));
      else resultStream = stream.unordered().filter(filters.stream().reduce(LongPredicate::and).orElse(t->true)).limit(limit);

      result = resultStream;
      resultIerator = resultStream.iterator();
    }

    @Override
    public void setDirection(boolean forward)
    {
      throw new UnsupportedOperationException("BigDataCursor can only be iterated forwards");
    }

    @Override
    public boolean first()
    {
      throw new UnsupportedOperationException("BigDataCursor does not support random accesses");
    }

    @Override
    public boolean last()
    {
      throw new UnsupportedOperationException("BigDataCursor does not support random accesses");
    }

    @Override
    public boolean next()
    {
      return resultIerator.hasNext();
    }

    @Override
    public void moveTo(int position, boolean changeDirection)
    {
      throw new UnsupportedOperationException("BigDataCursor does not support random accesses");
    }

    @Override
    public int count()
    {
      throw new UnsupportedOperationException("BigDataCursor does not know it's size");
    }

    @Override
    public long get()
    {
      return resultIerator.nextLong();
    }

    @Override
    public long get(int position)
    {
      throw new UnsupportedOperationException("BigDataCursor does not support random accesses");
    }

    @Override
    public void sort(SortOrder sortOrder)
    {
      throw new UnsupportedOperationException("BigDataCursor does not support sorting");
    }

    /**
     * StreamCursor result can only be consumed once (as a stream or iterating with get()).
     */
    @Override
    public LongStream getResult()
    {
      return result;
    }
  }

  /**
   * The big data cursor that supports sorting by keeping only Top (according to sortOrder) N (defined by limit) search results
   * @author olli
   *
   */
  public static class TopNCursor extends BasicCursor
  {
    public TopNCursor(LongStream stream, Collection<LongPredicate> filters, SortOrder sortOrder, int limit)
    {
      super(new long[0]);
      if (sortOrder == null || limit<0 || limit == Integer.MAX_VALUE) throw new IllegalArgumentException("Top N needs sort order (top) and limit (n)");

      LongStream resultStream;
      if (filters == null || filters.isEmpty()) resultStream = stream;
      else resultStream = stream.unordered().parallel().filter(filters.stream().reduce(LongPredicate::and).orElse(t->true));

      TopN topN = new TopN(limit, sortOrder);

      resultStream.forEach(number -> topN.offer(number));

      result = topN.finished();
    }

    protected static class TopN
    {
      SortOrder sortOrder;

      protected long[] topN;
      int firstLength;

      final long[] queue;
      int queueLength;


      public TopN (int N, SortOrder sortOrder)
      {
        this.topN = new long[N];
        this.queue = new long[N];
        this.sortOrder = sortOrder;
        sortOrder.reversed = true;
        firstLength = 0;
      }

      public void offer(long number)
      {
        if (firstLength < topN.length)
        {
          topN[firstLength++] = number;
          if (firstLength == topN.length)
          {
            LongArrays.sort(topN, sortOrder, 0, firstLength-1);
            queueLength = 0;
          }
          return;
        }

        if (sortOrder.compare(number, topN[0])<0) return;

        if (sortOrder.compare(number, topN[1])<0)
        {
          topN[0] = number;
          return;
        }

        queue[queueLength++] = number;
        if (queueLength == queue.length) recreateTopN();
      }

      public long[] finished()
      {
        sortOrder.reversed = false;

        if (firstLength < topN.length)
        {
          long[] result = new long[firstLength];
          System.arraycopy(topN, 0, result, 0, firstLength);
          LongArrays.sort(result, sortOrder);
          topN = null;
          return result;
        }
        else
        {
          if (queueLength>0) recreateTopN();
          LongArrays.sort(topN, sortOrder);
          queueLength = -1000000;
          return topN;
        }
      }

      protected void recreateTopN()
      {
        LongArrays.sort(queue, sortOrder, 0, queueLength-1);

        topN = LongArrays.merge(topN.length, topN, 0, topN.length-1, queue, 0, queueLength-1, sortOrder);

        queueLength = 0;
      }
    }
  }
}
