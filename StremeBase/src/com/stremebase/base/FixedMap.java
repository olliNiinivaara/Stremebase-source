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

import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;
import com.stremebase.file.FileManager.ValueSlot;


/**
 * FixedMap map is the abstract base class for all maps having fixed size arrays as values. 
 *
 */
public abstract class FixedMap
{
  protected final String mapName;
  protected final int nodeSize;
  protected final boolean persisted;

  protected Indexer indexer; 

  protected final TreeMap<Long, KeyFile> keyFiles = new TreeMap<>();
  protected final HashMap<Long, ValueFile> valueFiles = new HashMap<>();

  protected long largestValueFileId;
  protected long largestKey = DB.NULL;

  protected final MapGetter mapGetter;


  /*
   * Used only internally.
   */
  public FixedMap(String mapName, int nodeSize, boolean persist)
  {						
    this.mapName = mapName+nodeSize;
    this.nodeSize = nodeSize;
    this.persisted = DB.isPersisted() && persist;
    this.mapGetter = new MapGetter(this);

    largestValueFileId = DB.fileManager.loadProperty(mapGetter);
  }

  public void addIndex(int indexType)
  {
    if (indexer!=null) return;
    indexer = new Indexer(indexType, this);
  }

  public void dropIndex()
  {
    if (indexer==null) return;
    indexer.clear();
    indexer = null;
  }

  /**
   * @return {@link #getSize()} as int 
   */
  public int size()
  {
    return (int)getSize();
  }

  /**
   * Returns whether there is any keys stored in this map
   * @return true, if the map is empty
   */		
  public boolean isEmpty()
  {
    return getSize()==0;
  }	


  /*public boolean containsKey(Object key)
	{
		return containsKey((long)key);
	}*/

  /**
   * Returns the unique name of the map. Essentially used internally.
   * @return the name of the map.
   */
  public String getMapName()
  {
    return mapName;
  }

  /**
   * Returns the largest key currently stored to the map.
   * You are strongly advised to use this method to generate keys in an auto-incrementing manner, because Stremebase is optimized to handle such keys efficiently. 
   * @return the largest key currently in the map, 0 if the map is empty.
   */
  public long getLargestKey()
  {
    if (largestKey==DB.NULL)
    {
      OptionalLong largest = keys(getSize()-1, Long.MAX_VALUE).max();
      largestKey = largest.isPresent() ? largest.getAsLong() : 0;			
    }
    return largestKey;
  }

  /**
   * Returns the number of keys stored in this map.
   * @return size of the map.
   * Note that this is a relatively slow operation. Avoid successive calls by storing the value to a temporary variable.
   */
  public long getSize()
  {
    long size = 0;
    long fileId = -1;
    while (true)
    {
      KeyFile file = DB.fileManager.getNextKeyFile(mapGetter, fileId);
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
    return keys(Long.MIN_VALUE, Long.MAX_VALUE);
  }

  /**
   *  Returns all keys between the bounds as a {@link LongStream}
   * @param lowestKey lowest key, inclusive. Use {@link Long#MIN_VALUE} to avoid lower bound.
   * @param highestKey highest key, inclusive. Use {@link Long#MAX_VALUE} to avoid upper bound.
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
    return StreamSupport.longStream(spliterator(Long.MIN_VALUE, Long.MAX_VALUE, true), true);
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
  public void commit()
  {
    if (persisted)
    {
      DB.fileManager.commit(mapGetter);
    }
    if (isIndexed()) indexer.commit();
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
    DB.fileManager.close(mapGetter);
    if (isIndexed()) indexer.close();
  }

  /**
   * Tells whether there's data associated with a given key
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
   * Tells whether there's a value associated with some key in this map.
   * @param value the value
   * @return true, if the value is stored somewhere
   */
  public boolean containsValue(long value)
  {
    return query(value, value).findAny().isPresent(); 
  }

  /*protected boolean containsValue(long key, long value)
	{
		return indexOf(key, 0, value) != -1;
	}*/

  /**
   * Deletes all data
   */
  public void clear()
  {
    DB.fileManager.clear(mapGetter);
    if (isIndexed()) indexer.clear();
    largestValueFileId = DB.NULL;
    largestKey = DB.NULL;
  }

  /**
   * Returns all keys that are associated with a value that matches the given bounds.
   * @param lowestValue lowest acceptable value, inclusive. Use {@link Long#MIN_VALUE} to avoid lower bound.
   * @param highestValue highest acceptable value, inclusive. Use {@link Long#MAX_VALUE} to avoid upper bound.
   * @return result of the query as a stream of keys
   */
  public LongStream query(long lowestValue, long highestValue)
  {			
    if (!isIndexed()) return scanningQuery(lowestValue, highestValue);
    return indexer.getKeysWithValueFromRange(lowestValue, highestValue);
  }

  public LongStream unionQuery(long...values)
  {     
    if (!isIndexed()) return scanningUnionQuery(values);
    return indexer.getKeysWithValueFromSet(values);
  }

  /**
   * @return Iterator over the keys
   */
  public PrimitiveIterator.OfLong keySetIterator()
  {
    return new KeySetIterator(Long.MIN_VALUE, Long.MAX_VALUE);
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
    if (parallel)	return Spliterators.spliterator(new KeySetIterator(lowestKey, highestKey), size(),
        java.util.Spliterator.DISTINCT | java.util.Spliterator.IMMUTABLE | java.util.Spliterator.NONNULL | java.util.Spliterator.ORDERED);
    else return Spliterators.spliteratorUnknownSize(new KeySetIterator(lowestKey, highestKey),
        java.util.Spliterator.DISTINCT | java.util.Spliterator.IMMUTABLE | java.util.Spliterator.NONNULL | java.util.Spliterator.ORDERED);
  }

  protected KeyFile getData(long key, boolean create)
  {     
    KeyFile result = create ? DB.fileManager.getKeyFile(mapGetter, KeyFile.fileId(key), nodeSize) : DB.fileManager.getKeyFile(mapGetter, KeyFile.fileId(key), DB.NULL);
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

  abstract public void remove(long key);
  abstract public LongStream values(long key);
  abstract protected void put(long key, int index, long value);
  abstract protected int indexOf(long key, int fromIndex, long value);
  abstract protected LongStream scanningQuery(long lowestValue, long highestValue);
  abstract protected LongStream scanningUnionQuery(long... values);

  //------------------------------------------------------------------------------

  protected long iteratedValue;

  private class KeySetIterator implements PrimitiveIterator.OfLong
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

      if (lowestKey==Long.MIN_VALUE)
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

    public boolean hasNext()
    {
      while (true)
      {				
        if (key == toKey || remaining == 0)
        {
          file = DB.fileManager.getNextKeyFile(FixedMap.this.mapGetter, fileId);
          if (file == null) return false;
          fileId = file.id;		
          if (file.fromKey+DB.db.KEYSTOAKEYFILE<key) continue;		
          key = file.fromKey-1;					
          toKey = key + DB.db.KEYSTOAKEYFILE;
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

          if (key>=lowestKey)
          {
            iteratedValue = file.read(file.base(key)+1);
            return true;
          }
        }
      }	
    }

    public void forEachRemaining(LongConsumer action)
    {
      while (hasNext()) action.accept(nextLong());
    }

    public long nextLong()
    {
      return key;
    }
  }
}

