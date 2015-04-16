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

import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.LongStream.Builder;

import com.stremebase.map.OneMap;
import com.stremebase.map.SetMap;


/**
 * Implements indexing.
 * For internal use only.
 */
public class Indexer
{
  public final byte type;

  private final StremeMap map;
  private final StremeMap posIndex;
  private boolean neg = false;
  private StremeMap negIndex;

  public Indexer(byte type, StremeMap map)
  {
    this.type = type;
    this.map = map;
    this.posIndex = createPosIndex(null);
  }

  public Indexer(byte type, StremeMap map, int cell)
  {
    if (type != DB.ONE_TO_ONE && type != DB.MANY_TO_ONE) throw new IllegalArgumentException("Single cell can only be indexed with DB.ONE_TO_ONE or DB.MANY_TO_ONE");
    this.type = type;
    this.map = map;
    this.posIndex = createPosIndex("_cell"+cell);
  }

  protected StremeMap createPosIndex(String cell)
  {
    StremeMap posi;
    if (cell == null) cell = "";
    if ((type == DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) posi = new OneMap(map.getMapName()+"_posIndex"+cell, map.persisted);
    else  if (type == DB.MANY_TO_ONE || type == DB.MANY_TO_MANY) posi = new SetMap(map.getMapName()+"_posIndex", DB.db.INITIALINDEXVALUESIZE, SetMap.SET, map.persisted);
    else  if (type == DB.MANY_TO_MULTIMANY) posi = new SetMap(map.getMapName()+"_posIndex", DB.db.INITIALINDEXVALUESIZE, SetMap.MULTISET, map.persisted);
    else throw new IllegalArgumentException("Unrecognized index type: "+type);
    return posi;
  }

  public void commit()
  {
    posIndex.commit();
    if (neg) negIndex.commit();
  }

  public boolean isEmpty()
  {
    if (!posIndex.isEmpty()) return false;
    if (!neg) return true;
    return (negIndex.isEmpty());
  }

  public LongStream getKeysWithValueFromRange(final long lowestValue, final long highestValue)
  {
    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) return getKeysWithValueFromRange_ONE_TO(lowestValue, highestValue);
    else if (type == DB.MANY_TO_ONE || type == DB.MANY_TO_MANY) return getKeysWithValueFromRange_MANY_TO(lowestValue, highestValue);
    else if (type==DB.MANY_TO_MULTIMANY) return indexQuery(lowestValue, highestValue, 1, Long.MAX_VALUE).mapToLong(entry -> {return entry.value;});
    else throw new IllegalArgumentException("Unrecognized index type: "+type);
  }

  public LongStream getKeysWithValueFromRange_ONE_TO(long lowestValue, final long highestValue)
  {
    if (lowestValue>=0 || !neg)
    {
      if (lowestValue<0) lowestValue = 0;
      return posIndex.keys(lowestValue, highestValue).map(value -> {return ((OneMap)posIndex).get(value);}).
          filter(key -> {return key!=DB.NULL;});
    }

    if (highestValue<0) return negIndex.keys(-highestValue, -lowestValue).map(value -> {return ((OneMap)posIndex).get(value);}).
        filter(key -> {return key!=DB.NULL;});

    LongStream negStream = negIndex.keys(1, -lowestValue).map(value -> {return ((OneMap)posIndex).get(value);}).
        filter(key -> {return key!=DB.NULL;});

    LongStream posStream = posIndex.keys(0, highestValue).map(value -> {return ((OneMap)posIndex).get(value);}).
        filter(key -> {return key!=DB.NULL;});

    return LongStream.concat(negStream, posStream);
  }

  public LongStream getKeysWithValueFromRange_MANY_TO(long lowestValue, final long highestValue)
  {
    if (lowestValue>=0 || !neg)
    {
      if (lowestValue<0) lowestValue = 0;
      return posIndex.keys(lowestValue, highestValue).flatMap(value -> {return ((SetMap)posIndex).values(value);});
    }

    if (highestValue<0) return negIndex.keys(-highestValue, -lowestValue).map(value -> {return ((OneMap)posIndex).get(value);}).
        filter(key -> {return key!=DB.NULL;});

    LongStream negStream = negIndex.keys(1, -lowestValue).map(value -> {return ((OneMap)posIndex).get(value);}).
        filter(key -> {return key!=DB.NULL;});

    LongStream posStream = posIndex.keys(0, highestValue).map(value -> {return ((OneMap)posIndex).get(value);}).
        filter(key -> {return key!=DB.NULL;});

    return LongStream.concat(negStream, posStream);
  }

  public Stream<SetMap.SetEntry> indexQuery(long lowestValue, final long highestValue, final long lowestCount, final long highestCount)
  {
    Stream<SetMap.SetEntry> negStream;
    Stream<SetMap.SetEntry> posStream = null;
    long positiveLowest = 0;
    long negativeHighest = -1;

    if (highestValue>=0)
    {
      if (lowestValue>0) positiveLowest = lowestValue;
      posStream = posIndex.keys(positiveLowest, highestValue).
          <Stream<SetMap.SetEntry>>mapToObj(value ->
          {
            return ((SetMap)posIndex).entries(value).filter(entry ->
            {
              return entry.attribute>=lowestCount && entry.attribute<=highestCount;
            });
          }).flatMap(stream -> {return stream;});
    }

    if (lowestValue>=0 || !neg) return posStream;

    if (highestValue<0) negativeHighest = highestValue;
    negStream = negIndex.keys(-negativeHighest, -lowestValue).
        <Stream<SetMap.SetEntry>>mapToObj(value ->
        {
          return ((SetMap)posIndex).entries(value).filter(entry ->
          {
            return entry.attribute>=lowestCount && entry.attribute<=highestCount;
          });
        }).flatMap(stream -> {return stream;});

    if (highestValue<0) return negStream;

    return Stream.concat(negStream, posStream);
  }

  public LongStream getKeysWithValueFromSet(final long... values)
  {
    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) return getKeysWithValueFromSet_ONE_TO(values);
    else if (type==DB.MANY_TO_MULTIMANY) return indexUnionQuery(values, 1, Long.MAX_VALUE).mapToLong(entry -> {return entry.value;});
    else return getKeysWithValueFromSet_MANY_TO(values);
  }

  public LongStream getKeysWithValueFromSet_ONE_TO(final long... values)
  {
    Builder b = LongStream.builder();

    for (long value: values)
    {
      if (value==DB.NULL) continue;
      if (value>=0)
      {
        long key = ((OneMap)posIndex).get(value);
        if (key!=DB.NULL) b.add(key);
      }
      else
      {
        if (!neg) continue;
        long key = ((OneMap)negIndex).get(-value);
        if (key!=DB.NULL) b.add(key);
      }
    }

    return b.build();
  }

  public LongStream getKeysWithValueFromSet_MANY_TO(final long... values)
  {
    return LongStream.of(values).flatMap(value ->
    {
      if (value==DB.NULL) return LongStream.empty();
      if (value>=0) return ((SetMap)posIndex).values(value);
      if (!neg) return LongStream.empty();
      return ((SetMap)negIndex).values(-value);
    });
  }

  public Stream<SetMap.SetEntry> indexUnionQuery(final long[] values, final long lowestCount, final long highestCount)
  {
    Stream.Builder<SetMap.SetEntry> b = Stream.builder();

    for (long value: values)
    {
      if (value==DB.NULL) continue;
      indexQuery(value, value, lowestCount, highestCount).forEach(entry -> b.add(entry));
    }
    return b.build();
  }

  public void index(long key, long newValue)
  {
    if (newValue==DB.NULL) throw new IllegalArgumentException("Value cannot be DB.NULL");

    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY))
    {
      if (newValue>=0) ((OneMap)posIndex).put(newValue, key);
      else
      {
        if (!neg)
        {
          negIndex = new OneMap(map.getMapName()+"_negIndex", map.persisted);
          neg = true;
        }
        ((OneMap)negIndex).put(-newValue, key);
      }
    }
    else if (newValue>=0) ((SetMap)posIndex).put(newValue, key);
    else
    {
      if (!neg)
      {
        byte setType = type == DB.MANY_TO_MULTIMANY ? SetMap.MULTISET : SetMap.SET;
        negIndex = new SetMap(map.getMapName()+"_negIndex", DB.db.INITIALINDEXVALUESIZE, setType, map.persisted);
        neg = true;
      }
      ((SetMap)negIndex).put(-newValue, key);
    }
  }

  public void remove(long key, long oldValue)
  {
    if (oldValue==DB.NULL) return;

    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY))
    {
      if (oldValue>=0) posIndex.remove(oldValue);
      else if (neg)negIndex.remove(-oldValue);
    }
    else if (oldValue>=0) ((SetMap)posIndex).remove(oldValue, key);
    else if (neg) ((SetMap)negIndex).remove(-oldValue, key);
  }

  public void clear()
  {
    posIndex.clear();
    if (negIndex!=null) negIndex.clear();
  }

  public void close()
  {
    posIndex.close();
    if (negIndex!=null) negIndex.close();
  }
}
