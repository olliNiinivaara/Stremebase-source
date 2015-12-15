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

import java.util.HashMap;
import java.util.List;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.Spliterator;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import com.stremebase.file.FileManager;
import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;
import com.stremebase.file.FileManager.ValueSlot;
import com.stremebase.map.ArrayMap;


/**
 * StremeMap map is the abstract base class for all maps.
 * <p>
 * For internal use only.
 * @author olli
 */
public abstract class StremeMap
{
  protected String mapName;
  protected int nodeSize;
  protected boolean persisted;

  protected Indexer indexer;

  protected final TreeMap<Long, KeyFile> keyFiles = new TreeMap<>();
  protected final HashMap<Long, ValueFile> valueFiles = new HashMap<>();

  protected long largestValueFileId;
  protected long largestKey = DB.NULL;

  protected MapGetter mapGetter;

  protected long keysToAKeyFile;

  protected FileManager fileManager;

  private boolean indexQueryIsSorted = true;


  /**
   * Basic initialization.
   * @param mapName name
   * @param catalog the source of property values
   */
  public void initialize(String mapName, Catalog catalog)
  {
    this.mapName = mapName;

    nodeSize = (int) catalog.getProperty(Catalog.NODESIZE, this);

    persisted = (boolean) catalog.getProperty(Catalog.PERSISTED, this);

    if (!persisted || mapName.contains("_posIndex") || mapName.contains("_negIndex"))
      keysToAKeyFile = (long) catalog.getProperty(Catalog.KEYSTOAMEMORYANDINDEXKEYFILE, this);
    else if (this.getClass()==ArrayMap.class) keysToAKeyFile = (long) catalog.getProperty(Catalog.KEYSTOAARRAYKEYFILE, this);
    else keysToAKeyFile = (long) catalog.getProperty(Catalog.KEYSTOAKEYFILE, this);



    mapGetter = new MapGetter(this);
    fileManager = catalog.db.fileManager;
    largestValueFileId = fileManager.loadProperty(mapGetter);
  }

  protected void addIndex(DB db, byte indexType)
  {
    if (indexer!=null)
    {
      if (indexType==DB.NO_INDEX) dropIndex();
      return;
    }
    indexer = new Indexer(db, this, indexType);
    if (!isEmpty() && (indexer.isEmpty())) reIndex();
  }

  protected void dropIndex()
  {
    if (indexer==null) return;
    indexer.clear();
    indexer = null;
  }

  /**
   * Returns whether there is any keys stored in this map
   * @return true, if the map is empty
   */
  public boolean isEmpty()
  {
    long fileId = -1;
    while (true)
    {
      KeyFile file = fileManager.getNextKeyFile(mapGetter, fileId);
      if (file==null) return true;
      if (file.size()>0) return false;
      fileId = file.id;
    }
  }

  /**
   * Returns the name of the map.
   * Essentially used internally.
   * @return the name of the map.
   */
  public String getMapName()
  {
    return mapName;
  }

  /**
   * Returns the largest key currently stored to the map.
   * Use this method to generate keys in an auto-incrementing manner, because Stremebase can only handle continuous key sequences efficiently.
   * @return the largest key currently in the map, 0 if the map is empty.
   */
  public long getLargestKey()
  {
    if (largestKey==DB.NULL)
    {
      OptionalLong largest = keys(getCount()-1, Long.MAX_VALUE).max();
      largestKey = largest.isPresent() ? largest.getAsLong() : 0;
    }
    return largestKey;
  }

  /**
   * Returns the number of keys stored in this map.
   * @return size of the map.
   * Note that this is a relatively slow operation. Avoid successive calls by storing the value to a temporary variable.
   */
  public long getCount()
  {
    long size = 0;
    long fileId = -1;
    while (true)
    {
      KeyFile file =fileManager.getNextKeyFile(mapGetter, fileId);
      if (file==null) return size;
      size+= file.size();
      fileId = file.id;
    }
  }

  /**
   * Tells whether the map is in-memory or persisted to disk.
   * @return true, if the map is persisted to disk.
   */
  public boolean isPersisted()
  {
    return persisted;
  }


  /**
   * Returns all keys as a {@link LongStream}
   * @return stream of keys
   */
  public LongStream keys()
  {
    return keys(DB.MIN_VALUE, DB.MAX_VALUE);
  }

  /**
   *  Returns all keys between the bounds as a {@link LongStream}
   * @param lowestKey lowest key, inclusive. Use {@link DB#MIN_VALUE} to avoid lower bound.
   * @param highestKey highest key, inclusive. Use {@link DB#MAX_VALUE} to avoid upper bound.
   * @return stream of keys
   */
  public LongStream keys(long lowestKey, long highestKey)
  {
    return StreamSupport.longStream(spliterator(lowestKey, highestKey, false), false);
  }

  /**
   *  Returns all keys as a parallel {@link LongStream}
   * @return parallel stream of keys
   */
  public LongStream keyset()
  {
    return StreamSupport.longStream(spliterator(DB.MIN_VALUE, DB.MAX_VALUE, true), true);
  }

  /**
   *  Removes all the keys that appear in the stream
   *  @param keys the keys to remove
   */
  public void remove(LongStream keys)
  {
    keys.sequential().forEach(key -> remove(key));
  }

  /**
   *  Commits the map to disk. If the map is in-memory, does nothing.
   */
  public void flush()
  {
    if (persisted) fileManager.flush(mapGetter);
    if (isIndexed()) indexer.flush();
  }

  /**
   * Tells whether the map is indexed
   * @return true, if the map is indexed
   */
  public boolean isIndexed()
  {
    return (indexer!=null);
  }

  /**
   *  Commits data about free spaces in files to disk. If the map is in-memory, does nothing.
   */
  public void close()
  {
    fileManager.close(mapGetter);
    if (isIndexed()) indexer.close();
  }

  /**
   * Tells whether there's data associated with a given key
   * Note that a non-removed multi-valued attribute exists even when all it's individual values are DB.NULL
   * @param key the key
   * @return true, if value exists
   */
  public boolean containsKey(long key)
  {
    KeyFile buf = getData(key, false);
    if (buf==null) return false;
    return buf.read(buf.base(key))==1;
  }

  /**
   * This is _not_ needed in single-threaded applications
   * @param key a key
   * @return true if could be reserved
   */
  public boolean reserveKey(long key)
  {
    KeyFile buf = getData(key, true);
    return buf.setActive(buf.base(key), true);
  }


  /**
   * Tells whether there's a value associated with some key in this map.
   * @param value the value
   * @return true, if the value is stored somewhere
   */
  public boolean containsValue(long value)
  {
    return query(value, value).findAny().isPresent();
  }

  /**
   * Deletes all data
   */
  public void clear()
  {
    fileManager.clear(mapGetter);
    if (isIndexed()) indexer.clear();
    largestValueFileId = DB.NULL;
    largestKey = DB.NULL;
  }

  /**
   * If true, keys returned by queries are sorted (important when calculating key intersections).
   * Setting to false improves performance
   * @param sort default value true
   */
  public void setIndexQueryIsSorted(boolean sort)
  {
    indexQueryIsSorted = sort;
  }

  /**
   * Tells whether keys returned by queries are sorted
   * @return true if sorted
   */
  public boolean isIndexQuerySorted()
  {
    return indexQueryIsSorted;
  }

  /**
   * Returns all keys that are associated with a value that matches the given bounds (RANGE and BETWEEN-queries).
   * @param lowestValue lowest acceptable value, inclusive. Use {@link DB#MIN_VALUE} to avoid lower bound.
   * @param highestValue highest acceptable value, inclusive. Use {@link DB#MAX_VALUE} to avoid upper bound.
   * @return result of the query as a stream of keys
   */
  public LongStream query(long lowestValue, long highestValue)
  {
    if (!isIndexed()) return scanningQuery(lowestValue, highestValue);
    if (indexQueryIsSorted) return indexer.getKeysForValuesInRange(lowestValue, highestValue).sorted();
    return indexer.getKeysForValuesInRange(lowestValue, highestValue);
  }

  /**
   * Returns all keys that are associated with any given value (OR-query).
   * @param values the acceptable values
   * @return result of the query as a stream of keys
   */
  public LongStream unionQuery(long... values)
  {
    if (!isIndexed()) return scanningUnionQuery(values);
    if (indexQueryIsSorted) return indexer.getKeysForValues(values).sorted();
    return indexer.getKeysForValues(values);
  }

  /**
   * Removes the value from everywhere
   * @param value the value to be removed
   */
  public void removeValue(long value)
  {
    query(value, value).forEach(key -> removeValue(key, value));
  }

  /**
   * Removes the value association with the given key
   * @param key the key
   * @param value the value
   */
  public abstract void removeValue(long key, long value);

  /**
   * @return Iterator over the keys
   */
  public PrimitiveIterator.OfLong keySetIterator()
  {
    return new KeySetIterator(DB.MIN_VALUE, DB.MAX_VALUE);
  }

  /**
   * Spliterator over the matching keys
   * @param lowestKey Lowest acceptable key
   * @param highestKey Highest acceptable key
   * @param parallel Set to true, if a parallel spliterator should be requested.
   * @return {@link Spliterator.OfLong}
   */
  public Spliterator.OfLong spliterator(long lowestKey, long highestKey, boolean parallel)
  {
    if (parallel)	return Spliterators.spliterator(new KeySetIterator(lowestKey, highestKey), getCount(),
        java.util.Spliterator.DISTINCT | java.util.Spliterator.IMMUTABLE | java.util.Spliterator.NONNULL | java.util.Spliterator.ORDERED);
    else return Spliterators.spliteratorUnknownSize(new KeySetIterator(lowestKey, highestKey),
        java.util.Spliterator.DISTINCT | java.util.Spliterator.IMMUTABLE | java.util.Spliterator.NONNULL | java.util.Spliterator.ORDERED);
  }

  protected KeyFile getData(long key, boolean create)
  {
    KeyFile result = create ? fileManager.getKeyFile(mapGetter, KeyFile.fileId(key, keysToAKeyFile), nodeSize, keysToAKeyFile) : fileManager.getKeyFile(mapGetter, KeyFile.fileId(key, keysToAKeyFile), DB.NULL, DB.NULL);
    if (create  && key>largestKey) largestKey = key;
    return result;
  }

  protected int getNodeSize()
  {
    return nodeSize;
  }

  protected TreeMap<Long, List<ValueSlot>> getFreeValueSlots()
  {
    return null;
  }

  /**
   * Reindexes the map.
   * Mainly for internal use (used when new index is added to a map that contains data).
   */
  abstract public void reIndex();

  /**
   * Removes the key from the map
   * @param key the key to remove
   */
  abstract public void remove(long key);

  /**
   * Returns values associated with a key as a {@link LongStream}
   * @param key the key
   * @return the values, or an empty stream
   */
  abstract public LongStream values(long key);

  /**
   * Count of values associated with the key
   * @param key the key
   * @return number of values
   */
  abstract public long getValueCount(long key);

  abstract protected void put(long key, int index, long value);
  abstract protected LongStream scanningQuery(long lowestValue, long highestValue);
  abstract protected LongStream scanningUnionQuery(long... values);

  //------------------------------------------------------------------------------

  protected class KeySetIterator implements PrimitiveIterator.OfLong
  {
    private long key;
    private long toKey;
    private long remaining;
    private Long fileId;
    private KeyFile file;
    private final long lowestKey;
    private final long highestKey;

    protected KeySetIterator(long lowestKey, long highestKey)
    {
      this.lowestKey = lowestKey;
      this.highestKey = highestKey;

      if (lowestKey==DB.MIN_VALUE)
      {
        key = DB.NULL;
        toKey = DB.NULL;
        fileId = (long) DB.NULL;
      }
      else
      {
        key = lowestKey;
        toKey = lowestKey;
      }
      fileId = (long) DB.NULL;
    }

    @Override
    public boolean hasNext()
    {
      while (true)
      {
        if (key == toKey || remaining == 0)
        {
          file = fileManager.getNextKeyFile(StremeMap.this.mapGetter, fileId);
          if (file == null) return false;
          fileId = file.id;
          if (file.fromKey+keysToAKeyFile<key) continue;
          key = file.fromKey-1;
          toKey = key + keysToAKeyFile;
          remaining = file.size();
          if (remaining==0)
          {
            key = toKey;
            continue;
          }
        }
        key++;
        if (key>highestKey) return false;

        if (file.read(file.base(key))==1)
        {
          remaining--;
          if (key>=lowestKey) return true;
        }
      }
    }

    @Override
    public void forEachRemaining(LongConsumer action)
    {
      while (hasNext()) action.accept(nextLong());
    }

    @Override
    public long nextLong()
    {
      return key;
    }
  }
}

