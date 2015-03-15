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
import java.util.stream.LongStream.Builder;

import com.stremebase.map.OneMap;
import com.stremebase.map.SetMap;

//import com.stremebase.containers.LongSetMap;


public class Indexer
{
  private final int type;
  private final FixedMap map;

  //private final int intitialCapacity;
  private final FixedMap posIndex;
  private boolean neg = false;
  private FixedMap negIndex;

  public Indexer(int type, FixedMap map)
  {
    this.type = type;
    this.map = map;
    if ((type == DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) posIndex = new OneMap(map.getMapName()+"_posIndex", map.persisted);
    else posIndex = null;
  }

  public Indexer(int type, FixedMap map, int cell)
  {
    this.type = type;
    this.map = map;
    if (type == DB.ONE_TO_ONE) posIndex = new OneMap(map.getMapName()+"_posIndex_cell"+cell, map.persisted);
    else  if (type == DB.MANY_TO_ONE) posIndex = new SetMap(map.getMapName()+"_posIndex_cell"+cell, SetMap.SET, map.persisted);
    else throw new IllegalArgumentException("Single cell can only be indexed with DB.ONE_TO_ONE or DB.MANY_TO_ONE");
  }

  public void commit()
  {
    posIndex.commit();
    if (neg) negIndex.commit();
  }

  public LongStream getKeysWithValueFromRange(long lowestValue, long highestValue)
  {
    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) return getKeysWithValueFromRange_ONE_TO_ONE(lowestValue, highestValue);
    else return LongStream.empty();

    /*if (lowestValue>=0 || !neg) return posIndex.keys(lowestValue, highestValue).flatMap(key -> {return posIndex.get(key);});
		if (highestValue<0) return negIndex.keys(-highestValue, -lowestValue).flatMap(key -> {return negIndex.get(key);});
		return LongStream.concat(negIndex.keys(0, -lowestValue).flatMap(key -> {return negIndex.get(key);}),
		 posIndex.keys(0, highestValue).flatMap(key -> {return posIndex.get(key);}));*/
  }

  public LongStream getKeysWithValueFromRange_ONE_TO_ONE(long lowestValue, long highestValue)
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

  public LongStream getKeysWithValueFromSet(long... values)
  {
    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) return getKeysWithValueFromSet_ONE_TO_ONE(values);
    else return LongStream.empty();

    /*if (lowestValue>=0 || !neg) return posIndex.keys(lowestValue, highestValue).flatMap(key -> {return posIndex.get(key);});
    if (highestValue<0) return negIndex.keys(-highestValue, -lowestValue).flatMap(key -> {return negIndex.get(key);});
    return LongStream.concat(negIndex.keys(0, -lowestValue).flatMap(key -> {return negIndex.get(key);}),
     posIndex.keys(0, highestValue).flatMap(key -> {return posIndex.get(key);}));*/
  }

  public LongStream getKeysWithValueFromSet_ONE_TO_ONE(long... values)
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
      else throw new UnsupportedOperationException("not implemented yet");
    }

    return b.build();
  }

  public void index(long key, long oldValue, long newValue)
  {
    if (oldValue==newValue) return;

    if (oldValue!=DB.NULL)
    {
      if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY))
      {        
        if (oldValue>=0) ((OneMap)posIndex).remove(oldValue);
        else if (neg)negIndex.remove(-oldValue);
      }
      else throw new UnsupportedOperationException("not implemented yet");
    }

    if (newValue==DB.NULL) return;

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
    else throw new UnsupportedOperationException("not implemented yet");

    /* if (newValue<0)
    {
      throw new UnsupportedOperationException("not implemented yet");
      if (!neg)
      { 				
        negIndex = new LongSetMap(map.getMapName()+"_negIndex", intitialSize, posIndex.multiset, false, map.isPersisted());
        neg = true;
      }
      negIndex.put(-newValue, key);
    }*/	
  }

  /*public void removeOneValue(long key, long value)
  {
    if (value<0)
    {
      //if (neg) negIndex.removeOneValue(-value, key);
    }
    //else posIndex.removeOneValue(value, key);
  }

  public void resetValue(long key, long value)
  {
    if (value<0)
    {
      //if (neg) negIndex.resetValue(-value, key);
    }
    //else posIndex.resetValue(value, key);
  }*/

  public void remove(long key)
  {
    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY))
    {        
      long value = ((OneMap)map).get(key);
      if (value==DB.NULL) return;
      if (value>=0) ((OneMap)posIndex).remove(value);
      else if (neg) ((OneMap)negIndex).remove(-value);
    }
    else throw new UnsupportedOperationException("not implemented yet");


    //map.values(key).distinct().forEach(value -> resetValue(key, value));
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
