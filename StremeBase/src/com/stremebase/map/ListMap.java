package com.stremebase.map;

import java.util.Arrays;
import java.util.stream.LongStream;

import com.stremebase.base.DB;
import com.stremebase.base.DynamicMap;

public class ListMap extends DynamicMap
{
  public ListMap(String mapName)
  {
    super(mapName, DB.db.INITIALCAPACITY, DB.isPersisted());
  }

  public ListMap(String mapName, int initialCapacity, boolean persist)
  {
    super(mapName, initialCapacity, persist);
  }

  public int getTailPosition(long key)
  {
    long result = super.get(key, 0);
    if (result==DB.NULL) return -1;
    return (int)result;
  }

  public void push(long key, long value)
  {
    int position = getTailPosition(key)+1;
    super.put(key, position+1, value);
    super.put(key, 0, position);
  }

  public long pop(long key)
  {
    int position = getTailPosition(key);
    while (position >= 0)
    {
      if (super.get(key, position+1)==DB.NULL) position--;
      else
      {
        super.put(key, 0, position-1);
        return super.get(key, position+1);
      }
    }
    super.put(key, 0, -1);
    return DB.NULL;
  }

  @Override
  public void put(long key, int index, long value)
  {
    super.put(key, index+1, value);
    if (index+1>getTailPosition(key)) super.put(key, 0, index+1);
  }

  @Override
  public long get(long key, int index)
  {
    return super.get(key, index+1);
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    LongStream.Builder b =  LongStream.builder();

    keys().filter(key ->
    {
      return values(key).anyMatch(value -> (value!= DB.NULL && value >= lowestValue && value<=highestValue));
    }).forEach(key -> b.add(key));

    return b.build();
  }

  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    Arrays.sort(values);
    LongStream.Builder b =  LongStream.builder();

    keys().filter(key ->
    {
      return values(key).anyMatch(value -> (Arrays.binarySearch(values, value)>=0 ? true : false));
    }).forEach(key -> b.add(key));

    return b.build();
  }

  @Override
  public LongStream values(long key)
  {
    return super.values(key, true);
  }
}
