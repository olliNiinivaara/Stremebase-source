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
import com.stremebase.util.LongArrays;


/**
 * Implements indexing.
 * <p>
 * For internal use only.
 * @author olli
 */
public class Indexer
{
  protected final Relation posiRelation;
  protected Relation negaRelation;

  public Indexer(DB db, StremeMap map, byte type)
  {
    if (type==DB.ONE_TO_MANY) type = DB.MANY_TO_ONE;
    else if (type==DB.MANY_TO_ONE) type = DB.ONE_TO_MANY;
    posiRelation = new Relation(db, map.mapName+"_pIndex", map.persisted, type);
  }

  public Indexer(DB db, StremeMap map, byte type, int cell)
  {
    posiRelation = new Relation(db, map.mapName+"_pIndex_cell"+cell, map.persisted, type);
  }

  public void flush()
  {
    posiRelation.flush();
    if (negaRelation!=null) negaRelation.flush();
  }

  public boolean isEmpty()
  {
    if (!posiRelation.isEmpty()) return false;
    if (negaRelation==null) return true;
    return (negaRelation.isEmpty());
  }

  public LongStream getKeysForValuesInRange(long lowestValue, final long highestValue)
  {
    if (lowestValue>=0 || negaRelation==null) return posiRelation.getValuesForArgumentsInRange(lowestValue, highestValue);

    if (lowestValue==Long.MIN_VALUE) lowestValue++;

    if (highestValue<0)
    {
      if (negaRelation==null) return LongStream.empty();
      return negaRelation.getValuesForArgumentsInRange(-highestValue, -lowestValue);
    }

    LongStream negaStream;
    if (negaRelation==null) negaStream = LongStream.empty();
    else negaStream = negaRelation.getValuesForArgumentsInRange(1, -lowestValue);
    LongStream posiStream = posiRelation.getValuesForArgumentsInRange(0, highestValue);
    return LongStream.concat(negaStream, posiStream);
  }

  public LongStream getKeysForValues(final long... values)
  {
    LongStream posiStream = posiRelation.getValues(values);
    if (negaRelation==null) return posiStream;
    LongStream negaStream = posiRelation.getValues(LongArrays.negate(values));
    return LongStream.concat(negaStream, posiStream);
  }

  public void index(long key, long value)
  {
    if (value==DB.NULL) return;

    if (value>=0) posiRelation.relate(value, key);
    else
    {
      if (negaRelation==null) createNegaRelation();
      negaRelation.relate(-value, key);
    }
  }

  protected void createNegaRelation()
  {

    String relationName = posiRelation.relationName.replace("_pIndex", "_nIndex");
    negaRelation = new Relation(posiRelation.db, relationName, posiRelation.persisted, posiRelation.type);
  }

  public void unIndex(long key, long value)
  {
    if (value==DB.NULL) return;

    if (value>=0) posiRelation.unRelate(value, key);
    else if (negaRelation!=null) negaRelation.unRelate(-value, key);
  }

  public void removeValue(long value)
  {
    if (value==DB.NULL) return;

    if (value>=0) posiRelation.removeArgument(value);
    else if (negaRelation!=null) negaRelation.removeArgument(-value);
  }

  public void removeKey(long key)
  {
    if (key==DB.NULL) return;

    posiRelation.removeValue(key);
    if (negaRelation!=null) negaRelation.removeValue(key);
  }

  public void clear()
  {
    posiRelation.clear();
    if (negaRelation!=null) negaRelation.clear();
  }

  public void close()
  {
    posiRelation.close();
    if (negaRelation!=null) negaRelation.close();
  }

  public long getKeyCount(long value)
  {
    if (value==DB.NULL) return DB.NULL;
    if (value>=0) return posiRelation.getValueCount(value);
    if (negaRelation==null) return 0;
    return negaRelation.getValueCount(-value);
  }
}
