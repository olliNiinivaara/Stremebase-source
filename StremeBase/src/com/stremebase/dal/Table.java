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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.LongPredicate;
import java.util.stream.LongStream;

import com.stremebase.base.DB;
import com.stremebase.util.SortOrder;

/**
 * Table is container for all fields that share common key (belong to the same database entity).
 * <p>
 * First field added acts as a primary field, which is linearly scanned for keys in unindexed queries
 * @author olli
 *
 */
@SuppressWarnings("rawtypes")
public class Table
{
  public static final Map<Integer, Table> tables = new HashMap<>();
  public static DB defaultDb;

  public final int id;
  public final String name;
  public DB tableDb;
  public Field primaryField;

  /**
   * Set this to true to generate big data cursors for result sets that do not fit in main memory
   */
  public boolean bigData = false;

  public final Collection<Field> fields = new ArrayList<Field>();
  public final Collection<TableRelation> fromThisRelations = new ArrayList<TableRelation>();
  public final Collection<TableRelation> toThisRelations = new ArrayList<TableRelation>();

  /**
   * Normally the first thing to do in an application after db is created
   * @param db The db that is used by default
   */
  public static void setDefaultDb(DB db)
  {
    defaultDb = db;
    Value.to = db.to;
  }

  /**
   * Used mainly internally
   * @param tableId the id
   * @return the table
   */
  public static Table getTable(int tableId)
  {
    return tables.get(tableId);
  }

  /**
   * Flushes everything but lexicon - db.flush() will flush everything
   */
  public static void flushAll()
  {
    for (Table table: tables.values()) table.flush();
  }

  /**
   * Creates a table using the default db
   * @param id must be unique for every table (start from zero)
   * @param name name of the table, used mainly internally
   */
  public Table(int id, String name)
  {
    this(defaultDb, id, name);
  }

  /**
   * Creates a table using another db. Using multiple stremebases makes sense, if you want distribute data to multiple hard drives and operate them in parallel
   * @param db the db
   * @param id the id
   * @param name the name
   */
  public Table(DB db, int id, String name)
  {
    if (id<0) throw new IllegalArgumentException(id+"=id must be greater than 0");
    if (id>KeySpace.spaceSize) throw new IllegalArgumentException(id+"=id must be less than spaceSize ="+KeySpace.spaceSize);
    if (tables.containsKey(id)) throw new IllegalArgumentException(id+"=id is already used by "+tables.get(id).name);
    if (db==null) throw new IllegalArgumentException("db is missing");
    this.tableDb = db;
    this.id = id;
    this.name = name;
    tables.put(id, this);
  }

  protected void addField(Field field)
  {
    fields.add(field);
    if (fields.size()==1) primaryField = field;
  }

  /**
   * Used only internally
   */
  public void addFromThisRelation(TableRelation relation)
  {
    fromThisRelations.add(relation);
  }

  /**
   * Used only internally
   */
  public void addToThisRelation(TableRelation relation)
  {
    toThisRelations.add(relation);
  }

  /**
   * Flushes all data related to the table
   */
  public void flush()
  {
    for (Field field: fields) if (field.isModified) field.flush();
    for (TableRelation relation: fromThisRelations) relation.flush();
    for (TableRelation relation: toThisRelations) relation.flush();
  }

  /**
   * Generates a search result cursor
   * @param fromStream Stream to filter. If there's no index to use, setting this to null will linearly scan the primary field keys
   * @param filters the filters
   * @param sortOrder the sort criteria
   * @param limit maximum size for the search result, -1 and Integer.MAX_VALUE denote unlimited
   * @return new cursor
   */
  public Cursor query(LongStream fromStream, Collection<LongPredicate> filters, SortOrder sortOrder, int limit)
  {
    if (fromStream==null) fromStream = primaryField.map.keys();
    return Cursor.getCursor(bigData, fromStream, filters, sortOrder, limit);
  }

  /**
   * Number of keys in this table (= in primary field). Relatively slow operation, use sparingly.
   * @return the number
   */
  public long count()
  {
    return primaryField.map.getCount();
  }

  /**
   * If the key already exists
   * @param key the key
   * @return whether there is data associated with the key in primary field
   */
  public boolean keyExists(long key)
  {
    return primaryField.map.containsKey(key);
  }

  /**
   * Removes the key from all fields and all relations
   * @param key the key
   */
  public void remove(long key)
  {
    for (Field field: fields) field.remove(key);
    //TODO to-remove called also from from...
    for (TableRelation relation: fromThisRelations) relation.removeFromTableKey(key);
    for (TableRelation relation: toThisRelations) relation.removeToTableKey(key);
  }
}
