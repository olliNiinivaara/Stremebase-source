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

import java.util.stream.LongStream;

import com.stremebase.base.DB;
import com.stremebase.base.Relation;

/**
 * Relates keys from two tables. Also indexes the inverted direction if needed. Tables will ensure that removals are cascaded to here.
 * <p>
 * Possible relation types found in DB: {@link com.stremebase.base.DB#ONE_TO_ONE}, {@link com.stremebase.base.DB#ONE_TO_MANY},
 * {@link com.stremebase.base.DB#MANY_TO_ONE}, {@link com.stremebase.base.DB#MANY_TO_MANY}
 * <p>
 * Cannot relate keys from more than two tables. 
 * @author olli
 *
 */
public class TableRelation
{
  public boolean isModified;

  /**
   * If true, modifications are immediately flushed to disk
   */
  public boolean autoFlush = false;

  protected Relation relation;
  protected final TableRelation invertedRelation;
  protected final Table fromTable;
  protected final Table toTable;
  protected final byte relationType;
  protected final boolean usingInvertedRelations;


  /**
   * Creates a new relation.
   * @param fromTable the from-end of the relation
   * @param toTable the to-end of the relation
   * @param relationType see db constants for the various options
   * @param usingInvertedRelations if true, will also handle the to-from directionality 
   */
  public TableRelation(Table fromTable, Table toTable, byte relationType, boolean usingInvertedRelations)
  {
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.relationType = relationType;
    this.usingInvertedRelations = usingInvertedRelations;

    fromTable.addFromThisRelation(this);
    toTable.addToThisRelation(this);


    if (usingInvertedRelations)
    {
      if (fromTable == toTable)
      {
        if (relationType!=DB.ONE_TO_ONE && relationType!=DB.MANY_TO_MANY) throw new IllegalArgumentException("Relation inside table must be symmetric, either ONE_TO_ONE or MANY_TO_MANY");
        invertedRelation = this;
      }
      else
      {
        byte invertedRelationType = relationType;
        if (relationType==DB.ONE_TO_MANY) invertedRelationType = DB.MANY_TO_ONE;
        else if (relationType==DB.MANY_TO_ONE) invertedRelationType = DB.ONE_TO_MANY;
        invertedRelation = new TableRelation(toTable, fromTable, invertedRelationType, false);
      }
    }
    else invertedRelation = null;

    relation = new Relation(fromTable.tableDb, fromTable.name+"_r_"+toTable.name, fromTable.primaryField.map.isPersisted(), relationType);
  }

  protected void setModified(boolean modified)
  {
    isModified = modified;
    if (isModified && autoFlush) flush();
  }

  /**
   * Flushes possible modifications to disk
   */
  public void flush()
  {
    if (isModified)
    {
      relation.flush();
      isModified = false;
      if (invertedRelation!=null && invertedRelation!=this) invertedRelation.flush();
    }
  }

  /**
   * Count of relations from the given key
   * @param fromTableKey the key
   * @return the count
   */
  public long count(long fromTableKey)
  {
    return relation.getValueCount(fromTableKey);
  }

  /**
   * Count of relations to the given key
   * @param toTableKey the key
   * @return the count
   */
  public long inverseCount(long toTableKey)
  {
    return invertedRelation.count(toTableKey);
  }

  /**
   * Stream of relations from the given key
   * @param fromTableKey the from-key
   * @return the stream of to-keys
   */
  public LongStream getAsStream(long fromTableKey)
  {
    if (fromTableKey==DB.NULL) return null;
    return relation.getValues(fromTableKey);
  }

  /**
   * Relations from the given key as an array of to-keys
   * @param fromTableKey the from-key
   * @return the to-keys
   */
  public long[] getAsLongArray(long fromTableKey)
  {
    return getAsStream(fromTableKey).toArray();
  }

  /**
   * Stream of relations to the given key
   * @param toTableKey the to-key
   * @return the stream of from-keys
   */
  public LongStream getInvertedAsStream(long toTableKey)
  {
    if (toTableKey==DB.NULL) return null;
    return invertedRelation.getAsStream(toTableKey);
  }

  /**
   * Relations to the given key as an array of from-keys
   * @param toTableKey the to-key
   * @return the from-keys
   */
  public long[] getInvertedAsLongArray(long toTableKey)
  {
    return invertedRelation.getAsLongArray(toTableKey);
  }

  /**
   * Creates new relations  
   * @param fromTableKey from key
   * @param toTableKeys to keys
   */
  public void relate(long fromTableKey, long... toTableKeys)
  {
    for (long toTableKey: toTableKeys)
    {
      relation.relate(fromTableKey, toTableKey);
      if (invertedRelation!=null) invertedRelation.relation.relate(toTableKey, fromTableKey);
    }
    setModified(true);
  }

  /**
   * Creates new relations
   * <p>
   * Note that it's ok to call this even if not usingInvertedRelations
   * @param toTableKey to key
   * @param fromTableKeys from keys
   */
  public void relateInverted(long toTableKey, long... fromTableKeys)
  {
    for (long fromTableKey: fromTableKeys)
    {
      relation.relate(fromTableKey, toTableKey);
      if (invertedRelation!=null) invertedRelation.relation.relate(toTableKey, fromTableKey);
    }
    setModified(true);
  }

  /**
   * Removes relations
   * @param fromTableKey from key
   * @param toTableKeys to keys
   */
  public void unRelate(long fromTableKey, long... toTableKeys)
  {
    for (long toTableKey: toTableKeys)
    {
      relation.unRelate(fromTableKey, toTableKey);
      if (invertedRelation!=null) invertedRelation.relation.unRelate(toTableKey, fromTableKey);
    }
    setModified(true);
  }

  /**
   * Removes relations
   * <p>
   * Note that it's ok to call this even if not usingInvertedRelations
   * @param toTableKey to key
   * @param fromTableKeys from keys
   */
  public void unrelateInverted(long toTableKey, long... fromTableKeys)
  {
    for (long fromTableKey: fromTableKeys)
    {
      relation.unRelate(fromTableKey, toTableKey);
      if (invertedRelation!=null) invertedRelation.relation.unRelate(toTableKey, fromTableKey);
    }
    setModified(true);
  }

  /**
   * Removes all relations from the given key
   * @param fromTableKey the key
   */
  public void removeFromTableKey(long fromTableKey)
  {
    relation.removeArgument(fromTableKey);
    if (invertedRelation!=null && invertedRelation!=this) invertedRelation.removeToTableKey(fromTableKey);
    setModified(true);
  }

  /**
   * Removes all relations to given key
   * @param toTableKey the key
   */
  public void removeToTableKey(long toTableKey)
  {
    relation.removeValue(toTableKey);
    if (invertedRelation!=null && invertedRelation!=this) invertedRelation.removeFromTableKey(toTableKey);
    setModified(true);
  }
}
