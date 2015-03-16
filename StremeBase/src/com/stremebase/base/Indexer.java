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
    this.posIndex = createPosIndex(null);

  }

  public Indexer(int type, FixedMap map, int cell)
  {
    if (type != DB.ONE_TO_ONE && type != DB.MANY_TO_ONE) throw new IllegalArgumentException("Single cell can only be indexed with DB.ONE_TO_ONE or DB.MANY_TO_ONE");
    this.type = type;
    this.map = map;
    this.posIndex = createPosIndex("_cell"+cell);
  }

  protected FixedMap createPosIndex(String cell)
  {
    FixedMap posi;
    if (cell == null) cell = "";
    if ((type == DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) posi = new OneMap(map.getMapName()+"_posIndex"+cell, map.persisted);
    else  if (type == DB.MANY_TO_ONE || type == DB.MANY_TO_MANY) posi = new SetMap(map.getMapName()+"_posIndex", SetMap.SET, map.persisted);
    else throw new UnsupportedOperationException("not implemented yet");
    return posi;
  }

  public void commit()
  {
    posIndex.commit();
    if (neg) negIndex.commit();
  }

  public LongStream getKeysWithValueFromRange(long lowestValue, long highestValue)
  {
    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) return getKeysWithValueFromRange_ONE_TO(lowestValue, highestValue);
    else if (type == DB.MANY_TO_ONE || type == DB.MANY_TO_MANY) return getKeysWithValueFromRange_MANY_TO(lowestValue, highestValue);
    else throw new UnsupportedOperationException("not implemented yet");

    /*if (lowestValue>=0 || !neg) return posIndex.keys(lowestValue, highestValue).flatMap(key -> {return posIndex.get(key);});
		if (highestValue<0) return negIndex.keys(-highestValue, -lowestValue).flatMap(key -> {return negIndex.get(key);});
		return LongStream.concat(negIndex.keys(0, -lowestValue).flatMap(key -> {return negIndex.get(key);}),
		 posIndex.keys(0, highestValue).flatMap(key -> {return posIndex.get(key);}));*/
  }

  public LongStream getKeysWithValueFromRange_ONE_TO(long lowestValue, long highestValue)
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

  public LongStream getKeysWithValueFromRange_MANY_TO(long lowestValue, long highestValue)
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

  public LongStream getKeysWithValueFromSet(long... values)
  {
    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY)) return getKeysWithValueFromSet_ONE_TO(values);
    else if (type == DB.MANY_TO_ONE || type == DB.MANY_TO_MANY) return getKeysWithValueFromSet_MANY_TO(values);
    else throw new UnsupportedOperationException("not implemented yet");

    /*if (lowestValue>=0 || !neg) return posIndex.keys(lowestValue, highestValue).flatMap(key -> {return posIndex.get(key);});
    if (highestValue<0) return negIndex.keys(-highestValue, -lowestValue).flatMap(key -> {return negIndex.get(key);});
    return LongStream.concat(negIndex.keys(0, -lowestValue).flatMap(key -> {return negIndex.get(key);}),
     posIndex.keys(0, highestValue).flatMap(key -> {return posIndex.get(key);}));*/
  }

  public LongStream getKeysWithValueFromSet_ONE_TO(long... values)
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

  public LongStream getKeysWithValueFromSet_MANY_TO(long... values)
  {
    return LongStream.of(values).flatMap(value ->
    {
      if (value>=0) return ((SetMap)posIndex).values(value);
      throw new UnsupportedOperationException("not implemented yet");
    });
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
    else if (type == DB.MANY_TO_ONE || type == DB.MANY_TO_MANY)
    {
      if (newValue>=0) ((SetMap)posIndex).put(newValue, key);
      else
      {
        if (!neg)
        {
          negIndex = new SetMap(map.getMapName()+"_negIndex", SetMap.SET, map.persisted);
          neg = true;
        }
        ((SetMap)negIndex).put(-newValue, key);
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

  public void remove(long key, long oldValue)
  {
    if (oldValue==DB.NULL) return;

    if ((type==DB.ONE_TO_ONE) || (type == DB.ONE_TO_MANY))
    {        
      if (oldValue>=0) posIndex.remove(oldValue);
      else if (neg)negIndex.remove(-oldValue);
    }
    else if (type == DB.MANY_TO_ONE || type == DB.MANY_TO_MANY)
    {
      if (oldValue>=0) ((SetMap)posIndex).remove(oldValue, key);
      else if (neg) ((SetMap)negIndex).remove(-oldValue, key);
    }
    else throw new UnsupportedOperationException("not implemented yet");
  }

  /* public void remove(long key, LongStream values)
  {
    if (posIndex instanceof OneMap)
    {
      values.forEach(value ->
      {
        if (value>=0) ((OneMap)posIndex).remove(value);
        else if (neg && value!=DB.NULL) ((OneMap)negIndex).remove(-value);
      });
    }
    else
    {   
      values.forEach(value ->
      {
        if (value>=0) ((SetMap)posIndex).remove(value, key);
        else if (neg  && value!=DB.NULL) ((SetMap)negIndex).remove(-value, key);
      });
    }
  }*/

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
