package com.stremebase.map;

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

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

  @Override
  public void reIndex()
  {
    indexer.clear();
    keys().forEach(key -> (values(key).forEach(value -> (indexer.index(key, value)))));
    indexer.commit();
  }

  public int getTailPosition(long key)
  {
    long result = super.get(key, 0);
    if (result==DB.NULL) return -1;
    return (int)result-1;
  }

  public void push(long key, long value)
  {
    int position = getTailPosition(key)+1;
    super.put(key, position+1, value);
    super.put(key, 0, position+1);
  }

  public long pop(long key)
  {
    int position = getTailPosition(key)+1;
    while (position > 0)
    {
      if (super.get(key, position)==DB.NULL) position--;
      else
      {
        super.put(key, 0, position-1);
        return super.get(key, position);
      }
    }
    super.put(key, 0, 0);
    return DB.NULL;
  }

  @Override
  public void put(long key, int index, long value)
  {
    super.put(key, index+1, value);
    if (index+1>getTailPosition(key)) super.put(key, 0, index+1);
  }

  public void put(long key, int index, long... values)
  {
    for (int i = 0; i<values.length; i++) super.put(key, index+i+1, values[i]);
    if (values.length>getTailPosition(key)) super.put(key, 0, values.length);
  }

  @Override
  public long get(long key, int index)
  {
    return super.get(key, index+1);
  }

  @Override
  public LongStream values(long key)
  {
    return super.values(key, true, true);
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    return keys().filter(key ->
    {
      return values(key).anyMatch(value -> (value!= DB.NULL && value >= lowestValue && value<=highestValue));
    });
  }

  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    Arrays.sort(values);

    return keys().filter(key ->
    {
      return values(key).anyMatch(value -> (Arrays.binarySearch(values, value)>=0 ? true : false));
    });
  }

  public Stream<SetMap.SetEntry> indexQuery(long lowestValue, long highestValue, long lowestCount, long highestCount)
  {
    if (!isIndexed()) throw new IllegalArgumentException("IndexQuery needs a DB.MANY_TO_MULTIMANY index.");
    if (indexer.type!=DB.MANY_TO_MULTIMANY) throw new IllegalArgumentException("IndexQuery works only with a DB.MANY_TO_MULTIMANY index.");

    if (isIndexQuerySorted()) return indexer.indexQuery(lowestValue, highestValue, lowestCount, highestCount).sorted();
    return indexer.indexQuery(lowestValue, highestValue, lowestCount, highestCount);
  }

  public Stream<SetMap.SetEntry> indexUnionQuery(long[] values, long lowestCount, long highestCount)
  {
    if (!isIndexed()) throw new IllegalArgumentException("indexUnionQuery needs a DB.MANY_TO_MULTIMANY index.");
    if (indexer.type!=DB.MANY_TO_MULTIMANY) throw new IllegalArgumentException("indexUnionQuery works only with a DB.MANY_TO_MULTIMANY index.");

    if (isIndexQuerySorted()) return indexer.indexUnionQuery(values, lowestCount, highestCount).sorted(); 
    return indexer.indexUnionQuery(values, lowestCount, highestCount);
  }

}
