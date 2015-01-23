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

package com.stremebase.map;

import java.util.stream.LongStream;
import java.util.stream.LongStream.Builder;

import com.stremebase.base.DB;
import com.stremebase.base.FixedMap;
import com.stremebase.file.KeyFile;


public class LongMap extends FixedMap
{						
  /**
   * Creates a new LongMap that acts a simple key-value store where one long value can be associated with a given key.
   * No need to use methods that operate with value indices or arrays.
   * The returned LongMap is not indexed.
   * The returned LongMap is persistent iff the database is.
   * @param mapName name for the map. Must be a database-wide unique value.
   */
  
  //* Same as calling <code>LongMap(mapName, 1, DB.NOINDEX, DB.Persisted());</code>
  public LongMap(String mapName)
	{
	  super(mapName, 2, DB.NOINDEX, DB.isPersisted());
	}
	
	/*public LongMap(String mapName, int arraySize)
	{
		super(mapName, arraySize+1, DB.MULTIINDEX, DB.Persisted());
	}
	
	public LongMap(String mapName, int indexType, boolean persist)
	{
		super(mapName, 2, indexType, persist);
	}
	
	public LongMap(String mapName, int arraySize, int indexType, boolean persist)
	{
		super(mapName, arraySize+1, indexType, persist);
	}*/
		
  /**
   * Returns the value at index 0
   * @param key the key
   * @return the value. If there's no value, returns {@link com.stremebase.base.DB#NULL}.
   */
  public long get(long key)
	{
		return get(key, 0);
	}
	
  /**
   * Returns values associated with a key to given array
   * @param key the key
   * @param array the array to put the values into.
   * If there's no value, throws IndexOutOfBoundsException
   * @see com.stremebase.base.FixedMap#containsKey(long key)
   */
  public void get(long key, long[] array)
	{
		KeyFile buf = getData(key, false);
		if (buf==null) throw new IndexOutOfBoundsException("Cannot get nonexistent values.");
		int base = buf.base(key);
		if (buf.read(base)==0) throw new IndexOutOfBoundsException("Cannot get nonexistent values.");
		int length = array.length>nodeSize-1 ? nodeSize-1 : array.length; 
		buf.readToArray(base+1, array, length);
	}
	
  /**
   * Associates a value with a key (overwrites existing first value).
   * Same as calling <code>put(key, 0, value);</code> 
   * @param key the key
   * @param value the value
   */
  public void put(long key, long value)
	{
		put(key, 0, value);
	}
	
  /**
   * Removes the given key from the map
   * @param key the key to be removed
   */
  public void remove(long key)
	{
		KeyFile buf = getData(key, false);
		if (buf==null) return;
		int base = buf.base(key);
	  if (!buf.setActive(base, false)) return;

	  if (isIndexed()) for (int i=1; i<nodeSize; i++) indexer.index(key, buf.read(base+i), DB.NULL); 
	}
	
  /**
   * Returns the first index of given value, starting from given position
   * @param key the key
   * @param value a value to be searched for
   * @param fromIndex position to start looking
   * @return index for the value if found, and -1 if not found.
   */
  public int indexOf(long key, int fromIndex, long value)
	{
		if (fromIndex>=getNodeSize()-1) return -1;
		KeyFile buf = getData(key, false);
		if (buf==null) return -1;
		int base = buf.base(key);
		if (buf.read(base)==0) return -1;
		for (int i=fromIndex+1; i<nodeSize; i++) if (buf.read(base+i)==value) return 0;
		return -1;
	}
	
  /**
   * Returns value at given index
   * @param key the key
   * @param index the position
   * @return the value, or {@link com.stremebase.base.DB#NULL} if key was nonexistent or index was out of bounds.
   */
	public long get(long key, int index)
	{
		if (index>=getNodeSize()-1) return DB.NULL;
		KeyFile buf = getData(key, false);
		if (buf==null) return DB.NULL;
		int base = buf.base(key);
		if (buf.read(base)==0) return DB.NULL;
		return buf.read(base+1+index);
	}
	
	/**
   * Puts a value to given index position.
   * @param key the key
   * @param value the value
   * @param index the position
   */
	public void put(long key, int index, long value)
	{				
		if (key<0) throw new IllegalArgumentException("Negative keys are not supported ("+key+")");
		if (index>=getNodeSize()-1) new IndexOutOfBoundsException(index+">"+(getNodeSize()-2));
		KeyFile buf = getData(key, true);
		int base = buf.base(key);
		boolean olds = !buf.setActive(base, true);
		long oldValue = DB.NULL;
		if (olds)	oldValue = buf.read(base+index+1);
		if (isIndexed()) indexer.index(key, oldValue, value);
		buf.write(base+1+index, value);
	}
	
	/**
   * Puts multiple values at once.
   * @param key the key
   * @param array the values
   * Same as calling <code>put(key, 0, array);</code>
   */
	public void put(long key, long[] array)
	{
		put(key, 0, array);
	}
	
	/**
   * Puts multiple values at once, starting from given position.
   * @param key the key
   * @param index the position
   * @param array the values
   */
	public void put(long key, int index, long[] array)
	{		
		if (index+array.length>nodeSize-1) throw new IndexOutOfBoundsException(array.length+" won't fit from "+index+" to "+(nodeSize-1));
		KeyFile buf = getData(key, true);
		int base = buf.base(key);
		boolean olds = !buf.setActive(base, true);
		long oldValue = DB.NULL;
		
	  if (isIndexed()) for (int i = 0; i < array.length; i++)
		{
			oldValue = olds ? buf.read(base+index+i+1) : DB.NULL;
			indexer.index(key, oldValue, array[i]);
		}
	  
		buf.write(base+1+index, array);
	}

	/**
   * Returns the array associated with a key as a {@link LongStream}
   * @param key the key
   * @return the values or an empty stream if there's no value
   */
	@Override
	public LongStream values(long key)
	{
		KeyFile buf = getData(key, false);
		if (buf==null) return LongStream.empty();
		int base = buf.base(key);
		Builder b = LongStream.builder();	
		for (int i=1; i<nodeSize; i++) b.add(buf.read(base+i));
		return b.build();
	}
	
	@Override
  protected Object getObject(long key)
  {
    return get(key);
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    Builder b = LongStream.builder();
    keys().filter(key -> values(key).anyMatch(value -> 
    {
      if (value<lowestValue || value>highestValue) return false;
      return true;
    })).forEach(key -> b.add(key));
    return b.build();
  }
}