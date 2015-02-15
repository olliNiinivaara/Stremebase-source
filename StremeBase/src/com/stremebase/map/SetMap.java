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

import com.stremebase.base.DB;
import com.stremebase.base.DynamicMap;
import com.stremebase.base.util.Cache;
import com.stremebase.base.util.Cache.CacheDropObserver;
import com.stremebase.file.FileManager.ValueSlot;
import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;

 
public class SetMap extends ListMap implements CacheDropObserver
{
  protected final boolean multiset;
  protected final Cache writeCache;
  
  public SetMap(String mapName)
  {
    super(mapName, 1000, DB.NOINDEX, DB.isPersisted());
    this.multiset = true;
    writeCache = new Cache(this);
  }
  
  public SetMap(String mapName, int minimumSize, boolean persist, boolean multiset)
  {
    super(mapName, minimumSize, DB.NOINDEX, persist);
    this.multiset = multiset;
    writeCache = new Cache(this);
  }

  private boolean committing;
  
  @Override
  public void commit()
  {
    committing = true;
    writeCache.notifyForAll();
    writeCache.clear();
    super.commit();
    committing = false;
  }
  
  protected long getLength(long key)
  {
    notify(key, writeCache.get(key));
    return super.getSize(key)/2; 
  }
    
  protected boolean containsValue(long key, long value)
  {
    return getCount(key, value)>0;
  }
  
  protected long getCount(long key, long value)
  {
    long fileCount = fileCount(key, value, 0);
    long[] set = (long[])writeCache.get(key);
    if (set==null) return fileCount;
    int pos = findPosition(set, (int)set[0], value);
    if (pos>set[0]) return fileCount;
    long count = fileCount + set[pos+1];
    if (count<0) count = 0;
    if (!multiset && count > 1) count = 1;
    return count; 
  }
  
  public LongStream values(long key)
  {
    long[] set = (long[])writeCache.get(key);
    if (set!=null) writeCached(key, set);
    
    final boolean[] isValue = new boolean[1];
    isValue[0] = true;
    final long[] theValue = new long[1]; 
    
    return super.values(key).filter(value ->
    {
      if (isValue[0])
      {
        theValue[0] = value;
        isValue[0] = false;
        return false;
      }
      else
      {
        isValue[0] = true;
        return value > 0 ? true : false;
      }
    }).map(value -> {return theValue[0];});
  }
  
  protected void put(long key, long value, long amount)
  { 
    if (key<0) throw new IllegalArgumentException("Negative keys are not supported ("+key+")");
    if (value==DB.NULL) throw new IllegalArgumentException("Value cannot be DB.NULL");
    if (!multiset)
    {
      if (amount<-1) amount = -1;
      if (amount>1) amount = 1;
    }
    
    if (fileCount(key, value, amount)!=DB.NULL) return;
            
    long[] set = (long[])writeCache.get(key);
    if (set==null)
    {
      set = new long[2+DB.db.MAXCACHEDSETSIZE*2];
      writeCache.put(key, set);
    }
    
    if (set[0]==0)
    {
      if (amount<0) amount = 0;
      set[0] = 2;
      set[2] = value;
      set[3] = amount;
      return;
    }
    
    int end = (int)set[0];
    
    if (set[end]<value)
    {
      if (amount<0) return;
      set[0]+=2;
      set[(int)set[0]] = value;
      set[(int)(set[0]+1)] = amount;
      if (set[0]+2==set.length)
      {
        writeCache.notifyForKey(key);
        writeCache.remove(key);
      }
      return;
    }
    
    int pos = findPosition(set, end, value);
      
    if (set[pos]==value)
    {
      if (amount==DB.NULL) set[pos+1] = 0;
      else
      {
        set[pos+1]+=amount;
        if (set[pos+1]<0) set[pos+1] = 0;
        if (!multiset && set[pos+1]>1) set[pos+1] = 1;
      }
    }
    else if (pos>end)
    {
      if (amount<0) return;
      set[0]+=2;
      set[pos] = value;
      set[pos+1]=amount;
      if (set[0]+2==set.length)
      {
        writeCache.notifyForKey(key);
        writeCache.remove(key);
      }
    }
    else
    {
      if (amount<0) return;
      System.arraycopy(set, pos, set, pos + 2, end - pos + 2);
      set[0]+=2;
      set[pos] = value;
      set[pos+1]=amount;
      if (set[0]+2==set.length)
      {
        writeCache.notifyForKey(key);
        writeCache.remove(key);
      }
    }
    //DB.out(Arrays.toString(set));
    
  }
  
  protected int findPosition(long[] array, int last, long element)
  {   
    int start = 2;
    int end = last+1;
    int test = -1;
    long value;
    
    while (start<end)
    {     
      test = (end + start) / 2;
      if (test % 2 !=0) test-=1;
      value = array[test];
      
      if (value == element) return test;      
      if (value < element) start = test+2;
      else end = test;
    }
    return start;
  }
      
  public void put(long key, long value)
  { 
    put(key, value, 1);
  }
        
  public void removeOneValue(long key, long value)
  {
    put(key, value, -1);
  }
  
  protected void resetValue(long key, long value)
  {
    put(key, value, DB.NULL);
  }
  
  @Override
  public void remove(long key)
  {
    writeCache.remove(key);
    super.remove(key);
  }
        
  public boolean notify(long key, Object cached)
  {   
    if (cached == null) return false;
    if (((long[])cached)[0]==0) return false; 
    writeCached(key, (long[])cached);
    return !committing;
  }
            
  protected void writeCached(long key, long[] cached)
  {               
    //TODO index
    
    KeyFile header = getData(key, true);
    int base = header.base(key);
    final long oldLength = super.getSize(key);
    header.setActive(base, true);
        
    final ValueSlot newSlot = DB.fileManager.getFreeSlot(mapGetter, cached[0]+oldLength);
    long newPos = newSlot.slotPosition;
    
    int cachePos = 2;
    long currentNew = cached[cachePos];
    
    long newLength = 0;
    
    long count;
    
    ListIterator li = new ListIterator(key, oldLength);
    while (li.hasNext())
    {
      long oldValue = li.nextLong();
      
      while (cachePos<=cached[0] && oldValue>currentNew)
      {
        count = cached[cachePos+1];
        if (count!=0)
        {
          newSlot.valueFile.write(newPos, currentNew);
          newSlot.valueFile.write(newPos+1, count);
          newPos+=2;
          newLength++;
        }
        cachePos+=2;
        if (cachePos<=cached[0]) currentNew = cached[cachePos];
      }
      
      if (cachePos>cached[0] || oldValue<currentNew)
      {
        li.hasNext();
        count = li.nextLong();
        if (count!=0)
        {
          newSlot.valueFile.write(newPos, oldValue);        
          newSlot.valueFile.write(newPos+1, count);
          newLength++;
        }
        newPos+=2;
        continue;
      }
      
      if (oldValue==currentNew)
      {
        li.hasNext();
        continue;
      }   
    }
    
    while (cachePos<=cached[0])
    {
      count = cached[cachePos+1];
      if (count!=0)
      {     
        newSlot.valueFile.write(newPos, currentNew);
        newSlot.valueFile.write(newPos+1, count);
        newPos+=2;
        newLength++;
      }
      cachePos+=2;
      if (cachePos<=cached[0]) currentNew = cached[cachePos];
    }
    
    createHeader(key, newLength*2, newSlot);
  }
      
  protected long fileCount(long key, long element, long change)
  {
    KeyFile header = getData(key, false);
    if (header==null) return DB.NULL;
    int base = header.base(key);
    int end = (int)header.read(base+DynamicMap.pLength-1);
    int valueBase = (int)header.read(base+DynamicMap.pSlotFilePosition); 
    
    ValueFile file = DB.fileManager.getValueFile(mapGetter, header.read(base+DynamicMap.pSlotFileId));
        
    int start = 0;
    int test;
    long value;
    
    while (start<end)
    {     
      test = (end + start) / 2;
      if (test % 2 !=0) test-=1;
    
      value = file.read(valueBase+test);
      
    //TODO indexing...
      
      if (value == element)
      {
        long oldCount = file.read(valueBase+test+1);
        long count = oldCount+change;
        if (change==DB.NULL || count<0) count = 0;
        if (!multiset && count>1) count = 1;
        if (count!=oldCount) file.write(valueBase+test+1, count);
        return count;
      }
      
      if (value < element) start = test+2;
      else end = test;
    } 
    return DB.NULL;
  }
}
