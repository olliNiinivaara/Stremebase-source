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

/**
 * Implements relations.
 * <p>
 * Possible relation types found in DB: {@link com.stremebase.base.DB#ONE_TO_ONE}, {@link com.stremebase.base.DB#ONE_TO_MANY},
 * {@link com.stremebase.base.DB#MANY_TO_ONE}, {@link com.stremebase.base.DB#MANY_TO_MANY}
 * <p>
 * For internal use only.
 * @author olli
 */
public class Relation
{
  public final String relationName;
  protected final byte type;
  protected final DB db;
  protected final boolean persisted;
  protected final StremeMap relationMap;


  public Relation(DB db, String relationName, boolean persisted, byte type)
  {
    this.db = db;
    this.type = type;
    this.persisted = persisted;
    this.relationName = relationName;
    this.relationMap = createRelationMap();
  }

  protected StremeMap createRelationMap()
  {
    StremeMap posi;

    if (db.catalog.mapExistsOnDisk(relationName))
    {
      posi = db.getMap(relationName);
      return posi;
    }

    byte setType;

    if ((type == DB.ONE_TO_ONE) || (type == DB.MANY_TO_ONE)) setType = 0;
    else  if (type == DB.ONE_TO_MANY || type == DB.MANY_TO_MANY) setType = SetMap.SET;
    else throw new IllegalArgumentException("Unrecognized index type: "+type);

    if (setType==0) db.defineMap(relationName, OneMap.class, db.props().add(Catalog.PERSISTED, persisted).build(), false);
    else  db.defineMap(relationName, SetMap.class, db.props().add(Catalog.PERSISTED, persisted).add(Catalog.SETTYPE, setType).build(), false);

    posi = db.getMap(relationName);
    //if (map!=null) db.catalog.registerIndex(map, type);

    return posi;
  }

  public void flush()
  {
    relationMap.flush();
  }

  public boolean isEmpty()
  {
    return (relationMap.isEmpty());
  }

  public LongStream getValuesForArgumentsInRange(long lowestArgument, final long highestArgument)
  {
    if (lowestArgument<0) lowestArgument = 0;
    if ((type==DB.ONE_TO_ONE) || (type == DB.MANY_TO_ONE)) return getValuesWithArgumentFromRange_TO_ONE(lowestArgument, highestArgument);
    else if (type == DB.ONE_TO_MANY || type == DB.MANY_TO_MANY) return getValuesWithArgumentFromRange_TO_MANY(lowestArgument, highestArgument);
    else throw new IllegalArgumentException("Unrecognized index type: "+type);
  }

  protected LongStream getValuesWithArgumentFromRange_TO_ONE(final long lowestArgument, final long highestArgument)
  {
    return relationMap.keys(lowestArgument, highestArgument).map(value -> {return ((OneMap)relationMap).get(value);}).filter(key -> {return key!=DB.NULL;});
  }

  protected LongStream getValuesWithArgumentFromRange_TO_MANY(final long lowestArgument, final long highestArgument)
  {
    //TODO: test, maybe SetMap never even returns null values...
    return relationMap.keys(lowestArgument, highestArgument).flatMap(value -> {return ((SetMap)relationMap).values(value);}).filter(value -> {return value!=DB.NULL;});
  }

  public LongStream getValues(final long... arguments)
  {
    if ((type==DB.ONE_TO_ONE) || (type == DB.MANY_TO_ONE)) return getValuesWithArgumentFromSet_TO_ONE(arguments);
    else return getValuesWithArgumentFromSet_TO_MANY(arguments);
  }

  protected LongStream getValuesWithArgumentFromSet_TO_ONE(final long... arguments)
  {
    Builder b = LongStream.builder();

    for (long argument: arguments)
    {
      if (argument<0) continue;
      long value = ((OneMap)relationMap).get(argument);
      if (value!=DB.NULL) b.add(value);
    }
    return b.build();
  }

  protected LongStream getValuesWithArgumentFromSet_TO_MANY(final long... arguments)
  {
    return LongStream.of(arguments).flatMap(argument ->
    {
      if (argument<0) return LongStream.empty();
      return ((SetMap)relationMap).values(argument);
    });
  }

  public void relate(long argument, long value)
  {
    if (argument<0 || value==DB.NULL) return;

    if ((type==DB.ONE_TO_ONE) || (type == DB.MANY_TO_ONE))  ((OneMap)relationMap).put(argument, value);
    else ((SetMap)relationMap).put(argument, value);
  }

  public void unRelate(long argument, long value)
  {
    if (argument<0 || value==DB.NULL) return;

    if ((type==DB.ONE_TO_ONE) || (type == DB.MANY_TO_ONE))  relationMap.remove(argument);
    else ((SetMap)relationMap).removeValue(argument, value);
  }

  public void removeArgument(long argument)
  {
    if (argument<0) return;
    relationMap.remove(argument);
  }

  public void removeValue(long value)
  {
    if (value==DB.NULL) return;
    relationMap.removeValue(value);
  }

  public void clear()
  {
    relationMap.clear();
  }

  public void close()
  {
    relationMap.close();
  }

  public long getValueCount(long argument)
  {
    return relationMap.getValueCount(argument);
  }

  public static void main(String[] args)
  {
    Relation r = new Relation(new DB(), "r", false, DB.MANY_TO_MANY);
    r.relate(1, 2000);
    r.relate(2, 200);
    r.relate(1, 200);
    r.getValues(1).forEach(v->System.out.println(v));
  }
}
