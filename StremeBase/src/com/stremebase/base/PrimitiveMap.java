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


public abstract class PrimitiveMap
{
	protected final String mapName;
	protected final int nodeSize;
	protected final boolean persisted;
	
	protected final Indexer indexer; 
	
	protected final TreeMap<Long, KeyFile> keyFiles = new TreeMap<>();
	protected final HashMap<Long, ValueFile> valueFiles = new HashMap<>();
	
	protected long largestValueFileId;
	protected long largestKey = DB.NULL;
	
	protected final MapGetter mapGetter;

		
	public PrimitiveMap(String mapName, int nodeSize, int indexType, boolean persist)
	{						
		this.mapName = mapName+nodeSize;
		this.nodeSize = nodeSize;
		this.persisted = DB.Persisted() && persist;
		this.mapGetter = new MapGetter(this);
		
		largestValueFileId = DB.fileManager.loadProperty(mapGetter);
		if (indexType==DB.SIMPLEINDEX) indexer = new Indexer(this, nodeSize, false, persist);
		else if (indexType==DB.MULTIINDEX) indexer = new Indexer(this, nodeSize, true, persist);
		else indexer = null;
		
	}
	
	public int size()
	{
		return (int)getSize();
	}
			
	public boolean isEmpty()
	{
		return getSize()==0;
	}	
	
	public boolean containsKey(Object key)
	{
		return containsKey((long)key);
	}
		
	public String getMapName()
	{
		return mapName;
	}
		
	public long getLargestKey()
	{
		if (largestKey==DB.NULL)
		{
			OptionalLong largest = keys(getSize()-1, Long.MAX_VALUE).max();
			largestKey = largest.isPresent() ? largest.getAsLong() : 0;
			
		}
		return largestKey;
	}
	
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

	public boolean isPersisted()
	{
		return persisted;
	}
	
	public LongStream keys()
	{
		return keys(DB.NULL+1, Long.MAX_VALUE);
	}
	
	public LongStream keys(long lowestKey, long highestKey)
	{
		return StreamSupport.longStream(spliterator(lowestKey, highestKey, false), false);
	}
			
	public void remove(LongStream keys)
	{
		keys.sequential().forEach(key -> remove(key));
	}
	
	public void commit()
	{
		if (persisted)
		{
			DB.fileManager.commit(mapGetter);
		}
		if (isIndexed()) indexer.commit();
	}
	
	public boolean isIndexed()
	{
		return (indexer!=null);
	}
	
	public void close()
	{	
		DB.fileManager.close(mapGetter);
		if (isIndexed()) indexer.close();
	}
		
	public boolean containsKey(long key)
	{
		KeyFile buf = getData(key, false);
		if (buf==null) return false;
		return buf.read(buf.base(key))==1;
	}
	
	public boolean containsValue(long value)
	{
		return query((long)value, ((long)value)).findAny().isPresent(); 
	}
	
	protected boolean containsValue(long key, long value)
	{
		return indexOf(key, 0, value) != -1;
	}
	
	public void clear()
	{
		DB.fileManager.clear(mapGetter);
		if (isIndexed()) indexer.clear();
		largestValueFileId = DB.NULL;
	}
		
	public LongStream query(long lowestValue, long highestValue)
	{			
		if (!isIndexed()) throw new UnsupportedOperationException("Query by value is only possible for indexed MultiMaps.");
		return indexer.keysContainingValue(lowestValue, highestValue);
	}
								
	protected KeyFile getData(long key, boolean create)
	{			
		KeyFile result = create ? DB.fileManager.getKeyFile(mapGetter, KeyFile.fileId(key), nodeSize) : DB.fileManager.getKeyFile(mapGetter, KeyFile.fileId(key), DB.NULL);
		if (create  && key>largestKey) largestKey = key;
		return result;
	}
	
	public PrimitiveIterator.OfLong keySetIterator()
	{
		return new KeySetIterator(Long.MIN_VALUE, Long.MAX_VALUE);
	}
	
	public Spliterator.OfLong spliterator(long lowestKey, long highestKey, boolean parallel)
	{
		if (parallel)	return Spliterators.spliterator(new KeySetIterator(lowestKey, highestKey), size(),
		 java.util.Spliterator.DISTINCT | java.util.Spliterator.IMMUTABLE | java.util.Spliterator.NONNULL | java.util.Spliterator.ORDERED);
		else return Spliterators.spliteratorUnknownSize(new KeySetIterator(lowestKey, highestKey),
		 java.util.Spliterator.DISTINCT | java.util.Spliterator.IMMUTABLE | java.util.Spliterator.NONNULL | java.util.Spliterator.ORDERED);
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
  abstract protected Object getObject(long key);
  
	
//------------------------------------------------------------------------------
	
	private class KeySetIterator implements PrimitiveIterator.OfLong
	{
		private long key;
		private long toKey;
	  private long remaining;
	  private Long fileId;
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
					KeyFile file = DB.fileManager.getNextKeyFile(PrimitiveMap.this.mapGetter, fileId);
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
				KeyFile buf = PrimitiveMap.this.getData(key, false);			
				
				if (buf.read(buf.base(key))==1)
				{
					remaining--;
					
					if (key>=lowestKey)
					{
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

