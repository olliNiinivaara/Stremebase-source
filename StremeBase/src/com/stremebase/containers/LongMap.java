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

package com.stremebase.containers;

import java.util.stream.LongStream;
import java.util.stream.LongStream.Builder;

import com.stremebase.base.DB;
import com.stremebase.base.PrimitiveMap;
import com.stremebase.file.KeyFile;


public class LongMap extends PrimitiveMap
{						
	public LongMap(String mapName)
	{
		super(mapName, 2, DB.SIMPLEINDEX, DB.Persisted());
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
		
	public long get(long key)
	{
		return get(key, 0);
	}
	
	public void get(long key, long[] array)
	{
		KeyFile buf = getData(key, false);
		if (buf==null) throw new IndexOutOfBoundsException("Cannot get from nonexistent array.");
		int base = buf.base(key);
		if (buf.read(base)==0) throw new IndexOutOfBoundsException("Cannot get from nonexistent array.");
		int length = array.length>nodeSize-1 ? nodeSize-1 : array.length; 
		buf.readToArray(base+1, array, length);
	}
	
	public void put(long key, long value)
	{
		put(key, 0, value);
	}
	
	public void remove(long key)
	{
		KeyFile buf = getData(key, false);
		if (buf==null) return;
		int base = buf.base(key);
	  if (!buf.setActive(base, false)) return;

	  if (isIndexed()) for (int i=1; i<nodeSize; i++) indexer.index(key, buf.read(base+i), DB.NULL); 
	}
	
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
	
	public long get(long key, int index)
	{
		if (index>=getNodeSize()-1) new IndexOutOfBoundsException(index+">"+(getNodeSize()-2));
		KeyFile buf = getData(key, false);
		if (buf==null) return DB.NULL;
		int base = buf.base(key);
		if (buf.read(base)==0) return DB.NULL;
		return buf.read(base+1+index);
	}
	
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
	
	public void put(long key, long[] array)
	{
		put(key, 0, array);
	}
	
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

	@Override
	protected Object getObject(long key)
	{
		return get(key);
	}

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
}