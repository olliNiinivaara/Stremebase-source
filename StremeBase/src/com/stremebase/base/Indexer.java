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

//import com.stremebase.containers.LongSetMap;


public class Indexer
{
	private final FixedMap map;
	@SuppressWarnings("unused")
	private final int intitialSize;
	//private final LongSetMap posIndex;
	private boolean neg = false;
	//private LongSetMap negIndex;
	
	public Indexer(FixedMap map, int intitialSize, boolean multiset, boolean indexed)
	{
		this.map = map;
		this.intitialSize = intitialSize;
		//posIndex = new LongSetMap(map.getMapName()+"_posIndex", intitialSize, multiset, false, map.persisted);
	}
	
	public void commit()
	{
		//posIndex.commit();
		//if (neg) negIndex.commit();
	}
	
	public LongStream keysContainingValue(long lowestValue, long highestValue)
	{
		return null;
		//db.null...
		/*if (lowestValue>=0 || !neg) return posIndex.keys(lowestValue, highestValue).flatMap(key -> {return posIndex.get(key);});
		if (highestValue<0) return negIndex.keys(-highestValue, -lowestValue).flatMap(key -> {return negIndex.get(key);});
		return LongStream.concat(negIndex.keys(0, -lowestValue).flatMap(key -> {return negIndex.get(key);}),
		 posIndex.keys(0, highestValue).flatMap(key -> {return posIndex.get(key);}));*/
	}
	
	public void index(long key, long oldValue, long newValue)
	{
		if (oldValue==newValue) return;
		
    if (oldValue!=DB.NULL)
    {
    	//if (oldValue<0) negIndex.removeOneValue(-oldValue, key);
    	//else posIndex.removeOneValue(oldValue, key);
    }
    
    if (newValue==DB.NULL) return;
    
    if (newValue<0)
		{
			if (!neg)
			{ 				
			  //negIndex = new LongSetMap(map.getMapName()+"_negIndex", intitialSize, posIndex.multiset, false, map.isPersisted());
			  neg = true;
			}
			//negIndex.put(-newValue, key);
		}
		//else posIndex.put(newValue, key);		
	}
	
	public void removeOneValue(long key, long value)
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
	}
		
	public void remove(long key)
	{
		map.values(key).distinct().forEach(value -> resetValue(key, value));
	}
	
	public void clear()
	{
		//posIndex.clear();
		//if (negIndex!=null) negIndex.clear();
	}
	
	public void close()
	{
		//posIndex.close();
		//if (negIndex!=null) negIndex.close();
	}
}
