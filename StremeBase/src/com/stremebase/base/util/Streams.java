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

package com.stremebase.base.util;

import java.util.Arrays;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.stremebase.base.DB;

/**
 * This class contains static method(s) for manipulating streams.
 */
public final class Streams 
{		
  protected static final Streams s = new Streams();

  private Streams() {}

  public static LongStream union(LongStream... streams)
  {
    int existing = 0;
    for (LongStream s: streams) if (s!=null) existing++;
    if (existing==0) return LongStream.empty();
    return StreamSupport.longStream(s.new StreamUnioner(existing, streams), false);
  }

  public static LongStream union(Stream<LongStream> streamstream)
  {
    return union(streamstream.toArray(LongStream[]::new));
  }

  /**
   * Returns intersection of the input streams. Fast, but input streams MUST be ordered.
   * @param streams the streams to be intersected
   * @return intersection of streams as a stream
   */
  public static LongStream intersection(LongStream... streams)
  {
    if (streams == null) return LongStream.empty();		
    for (LongStream stream: streams) if (stream == null) return LongStream.empty();
    if (streams.length==1) return streams[0];
    return StreamSupport.longStream(s.new StreamIntersector(streams), false);
  }

  class StreamIntersector implements Spliterator.OfLong
  {
    PrimitiveIterator.OfLong[] streams;
    long[] keys;
    long candidate = DB.NULL;

    StreamIntersector(LongStream... inputStreams)
    {
      candidate = DB.NULL;

      this.streams = new PrimitiveIterator.OfLong[inputStreams.length]; 
      for (int i=0; i<inputStreams.length; i++) this.streams[i] = inputStreams[i].iterator();

      keys = new long[inputStreams.length];
      Arrays.fill(keys, DB.NULL+1);
    }

    @Override
    public long estimateSize()
    {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics()
    {
      return DISTINCT | IMMUTABLE | NONNULL | ORDERED;
    }

    @Override
    public java.util.Spliterator.OfLong trySplit()
    {
      return null;
    }

    @Override
    public boolean tryAdvance(LongConsumer action)
    {
      outerloop: while (true)
      {
        for (int i = 0; i<streams.length; i++)
        {
          if (keys[i] == candidate) continue;
          if (keys[i]<candidate || candidate==DB.NULL)
          {	
            if (!streams[i].hasNext()) return false;
            keys[i] = streams[i].nextLong();
            if (keys[i]>candidate) candidate = keys[i];
            continue outerloop;
          }
        }
        action.accept(candidate++);
        return true;
      }
    }
  }

  class StreamUnioner implements Spliterator.OfLong
  {

    PrimitiveIterator.OfLong[] streams;
    long[] waitingKeys;
    int currentStream = -1;

    StreamUnioner(int existing,LongStream... inputStreams)
    {
      this.streams = new PrimitiveIterator.OfLong[existing];
      int j = 0;
      for (LongStream inputStream : inputStreams)
        if (inputStream!=null) this.streams[j++] = inputStream.iterator();

      waitingKeys = new long[inputStreams.length];

      for (int i = 0; i<streams.length; i++)
      {
        if (!streams[i].hasNext()) waitingKeys[i] = DB.NULL;    
        else
        {
          waitingKeys[i] = streams[i].nextLong();
          if (currentStream==-1) currentStream = i;
          else if (waitingKeys[i]<waitingKeys[currentStream]) currentStream = i;
        }
      }
    }

    @Override
    public long estimateSize()
    {
      return Long.MAX_VALUE;
    }

    @Override
    public int characteristics()
    {
      return DISTINCT | IMMUTABLE | NONNULL | ORDERED;
    }

    @Override
    public java.util.Spliterator.OfLong trySplit()
    {
      return null;
    }

    @Override
    public boolean tryAdvance(LongConsumer action)
    {
      if (currentStream == -1) return false;
      final long nextKey = waitingKeys[currentStream];
      if (nextKey == DB.NULL) return false;
      if (!streams[currentStream].hasNext()) waitingKeys[currentStream] = DB.NULL;
      else waitingKeys[currentStream] = streams[currentStream].nextLong();

      for (int i = 0; i<streams.length; i++)
        if (waitingKeys[i]!=DB.NULL && (waitingKeys[currentStream] == DB.NULL || waitingKeys[i]<waitingKeys[currentStream])) currentStream = i;

      action.accept(nextKey);
      return true;
    }
  }
}
