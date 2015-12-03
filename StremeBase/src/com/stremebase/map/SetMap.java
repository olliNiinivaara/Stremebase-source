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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.stremebase.base.Catalog;
import com.stremebase.base.DB;
import com.stremebase.base.DynamicMap;
import com.stremebase.file.FileManager.ValueSlot;
import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;


/**
 * SetMap associates a key with a set or a bag of values.
 * <p>
 * Set the set type in constructor:
 * <p>
 * SetMap.SET: An ordinary set
 * <p>
 * SetMap.MULTISET: A bag - occurrences are counted
 * <p>
 * SetMap.ATTRIBUTEDSET: Values can be associated with an arbitrary tag (for example type or weight of value)
 * @author olli
 */
public class SetMap extends DynamicMap
{
  /**
   *Set 
   */
  public static final byte SET = 1;

  /**
   *Bag 
   */
  public static final byte MULTISET = 2;

  /**
   * Tagged values
   */
  public static final byte ATTRIBUTEDSET = 3;

  protected byte type;
  protected SetCache setCache;
  protected int maxCachedSetValueEntries;
  protected long[] overwriterCache;

  protected static final long NOTWRITTEN = DB.NULL;
  protected static final long WRITTEN = 0;

  /**
   * SetEntry contains not only key and value, but also an attribute
   * for MULTISETs, the attribute is the count of values
   * for ATTRIBUTEDSETs, you can assign any value but DB.NULL
   * <p>
   * SetEntries are compared by attributes
   */
  public class SetEntry implements Comparable<SetEntry>
  {
    public final long key;
    public final long value;
    public final long attribute;

    public SetEntry(long key, long value, long attribute)
    {
      this.key = key;
      this.value = value;
      this.attribute = attribute;
    }

    @Override
    public int compareTo(SetEntry e)
    {
      if (attribute < e.attribute) return -1;
      if (attribute == e.attribute) return 0;
      return 1;
    }

    @Override
    public String toString()
    {
      return key+":"+value+":"+attribute;
    }
  }

  @Override
  public void initialize(String mapName, Catalog catalog)
  {
    super.initialize(mapName, catalog);
    type = (byte) catalog.getProperty(Catalog.SETTYPE, this);
    setCache = new SetCache(this, (int) catalog.getProperty(Catalog.MAXCACHEDSETSIZE, this));
    maxCachedSetValueEntries = (int) catalog.getProperty(Catalog.MAXCACHEDSETVALUEENTRIES, this);
    overwriterCache = new long[2+maxCachedSetValueEntries*2];
  }

  @Override
  public void flush()
  {
    setCache.flushAll();
    super.flush();
  }

  @Override
  protected void addIndex(DB db, byte indexType)
  {
    setCache.flushAll();
    super.addIndex(db, indexType);
  }

  @Override
  public void reIndex()
  {
    indexer.clear();
    flush();
    keys().forEach(key -> (values(key).forEach(value -> (indexer.index(key, value)))));
    indexer.flush();
  }

  /**
   * Returns the size of the set, including DB.NULL values
   */
  @Override
  public long getValueCount(long key)
  {
    if (key==DB.NULL) return 0;
    setCache.flush(key);
    return super.getValueCount(key)/2;
  }

  /**
   * Tells if the set for a key contains a given value
   * @param key the key
   * @param value the key
   * @return result
   */
  public boolean containsValue(long key, long value)
  {
    long attribute = getAttribute(key, value);
    return (attribute!=DB.NULL &&(type!=MULTISET || attribute!=0));
  }

  /**
   * Returns the attribute associated with a key
   * For multisets, the count of values
   * For attributedsets, something you have put there
   * @param key the key
   * @param value the value
   * @return the attribute or DB.NULL
   */
  public long getAttribute(long key, long value)
  {
    long[] set = setCache.get(key);
    long cacheCount = 0;
    if (set!=null)
    {
      int pos = findPosition(set, (int)set[0], value);
      if (pos<=set[0])
      {
        if (set[pos+1] == DB.NULL) return DB.NULL;
        if (type!=MULTISET) return set[pos+1];
        cacheCount = set[pos+1];
      }
    }

    long fileAttribute = fileAttribute(key, value, DB.NULL, false);
    if (type==MULTISET)
    {
      if (fileAttribute==DB.NULL) return cacheCount;
      return fileAttribute+cacheCount;
    }
    return fileAttribute;
  }

  /**
   * The SetEntries associated with a key
   * @param key the key
   * @return stream of entries
   */
  public Stream<SetEntry> entries(long key)
  {
    setCache.flush(key);

    final byte[] index = new byte[1];
    index[0] = 0;
    final long[] entry = new long[2];

    return super.values(key, false).filter(value ->
    {
      if (index[0]==0)
      {
        entry[0] = value;
        index[0] = 1;
        return false;
      }
      else
      {
        entry[1] = value;
        index[0] = 0;
        return value != DB.NULL ? true : false;
      }
    }).mapToObj(value -> {return new SetEntry(key, entry[0], entry[1]);});
  }

  /**
   * The values associated with a key
   */
  @Override
  public LongStream values(long key)
  {
    setCache.flush(key);

    final boolean[] isValue = new boolean[1];
    isValue[0] = true;
    final long[] theValue = new long[1];

    return super.values(key, false).filter(value ->
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
        return value != DB.NULL ? true : false;
      }
    }).map(value -> {return theValue[0];});
  }

  /*public LongStream attributes(long key)
  {
    throw new UnsupportedOperationException("never implemented...");
  }*/

  /**
   * Put for ATTRIBUTEDSETs
   * @param key the key
   * @param value the value
   * @param attribute the attribute
   */
  public void put(long key, long value, long attribute)
  {
    if (key<0) throw new IllegalArgumentException("Negative keys are not supported ("+key+")");
    if (value==DB.NULL) throw new IllegalArgumentException("Value cannot be DB.NULL");

    long[] set = setCache.get(key);
    if (set==null)
    {
      set = new long[2+maxCachedSetValueEntries*2];
      setCache.put(key, set);
    }

    if (set[0]==0)
    {
      set[0] = 2;
      set[2] = value;
      set[3] = attribute;
      return;
    }

    int end = (int)set[0];

    if (set[end]<value)
    {
      set[0]+=2;
      set[(int)set[0]] = value;
      set[(int)(set[0]+1)] = attribute;
      if (set[0]+2==set.length) setCache.flush(key);
      return;
    }

    int pos = findPosition(set, end, value);

    if (set[pos]==value)
    {
      if (attribute == DB.NULL) set[pos+1] = attribute;
      else set[pos+1] = type == MULTISET ? set[pos+1] + attribute : attribute;
      return;
    }

    if (pos<=end) System.arraycopy(set, pos, set, pos + 2, end - pos + 2);

    set[0]+=2;
    set[pos] = value;
    set[pos+1]=attribute;
    if (set[0]+2==set.length) setCache.flush(key);
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

  /**
   * Puts a new value to a key's set
   * @param key the key
   * @param value the value
   */
  public void put(long key, long value)
  {
    put(key, value, 1);
  }


  /**
   * A helper method to put multiple entries at once to an ATTRIBUTEDSET
   * @param entries an array of arrays of 3 longs: key, value, and attribute
   */
  public void put(long[]... entries)
  {
    if (type != ATTRIBUTEDSET) throw new UnsupportedOperationException("Only ATTRIBUTEDSETs have attributes");
    for (long[] entry: entries) put(entry[0], entry[1], entry[2]);
  }

  /**
   * Removes a value from set. For MULTISET, this means decreasing count by one.
   * @param key the key
   * @param value the value
   */
  public void removeOne(long key, long value)
  {
    if (type == MULTISET) put(key, value, -1); else put(key, value, DB.NULL);
  }

  @Override
  public void removeValue(long key, long value)
  {
    put(key, value, DB.NULL);
  }

  /**
   * Sets an attribute for a value. Works only with ATTRIBUTEDSET.
   * @param key the key
   * @param value the value
   * @param attribute the attribute
   */
  public void setAttribute(long key, long value, long attribute)
  {
    if (type != ATTRIBUTEDSET) throw new UnsupportedOperationException("Only ATTRIBUTEDSETs have attributes");
    put(key, value, attribute);
  }

  @Override
  public void remove(long key)
  {
    setCache.remove(key);
    super.remove(key);
  }

  protected void writeCached(long key, long[] cached)
  {
    if (cached == null) return;
    if (cached[0]==0) return;

    KeyFile header = getData(key, false);

    if (header!=null)
    {
      overWrite(key, cached);
      if (cached[0] == DB.NULL) return;
    }

    header = getData(key, true);
    int base = header.base(key);
    final long oldLength = super.getValueCount(key);
    header.setActive(base, true);

    final ValueSlot newSlot = fileManager.getFreeSlot(mapGetter, cached[0]+oldLength+2);

    long newPos = newSlot.slotPosition;

    int cachePos = 2;
    long newLength = 0;

    if (oldLength>0)
    {
      ListIterator li = new ListIterator(key, oldLength, false);
      while (li.hasNext())
      {
        long oldValue = li.nextLong();

        li.hasNext();
        long oldAttribute = li.nextLong();

        while (cachePos<cached[0]+2 && oldValue>=cached[cachePos])
        {
          if (cached[cachePos+1]==DB.NULL)
          {
            cachePos+=2;
            continue;
          }
          newSlot.valueFile.write(newPos, cached[cachePos]);
          newSlot.valueFile.write(newPos+1, cached[cachePos+1]);
          newPos+=2;
          newLength++;
          if (isIndexed()) indexer.index(key, cached[cachePos]);
          cachePos+=2;
        }

        if (oldAttribute==DB.NULL) continue;
        newSlot.valueFile.write(newPos, oldValue);
        newSlot.valueFile.write(newPos+1, oldAttribute);
        newPos+=2;
        newLength++;
      }
    }

    while (cachePos<cached[0]+2)
    {
      if (cached[cachePos+1]!=DB.NULL)
      {
        newSlot.valueFile.write(newPos, cached[cachePos]);
        newSlot.valueFile.write(newPos+1, cached[cachePos+1]);
        newPos+=2;
        newLength++;
        if (isIndexed()) indexer.index(key, cached[cachePos]);
      }
      cachePos+=2;
    }
    createHeader(key, newLength*2, newSlot);
  }

  protected void overWrite(long key, long[] cached)
  {
    System.arraycopy(cached, 0, overwriterCache, 0, (int) cached[0]+2);
    cached[0] = 0;

    for (int i=2; i<overwriterCache[0]+2; i+=2)
    {
      long written = fileAttribute(key, overwriterCache[i], overwriterCache[i+1], true);
      if (written==NOTWRITTEN)
      {
        cached[(int) cached[0]+2] = overwriterCache[i];
        cached[(int) cached[0]+3] = overwriterCache[i+1];
        cached[0]+=2;
      }
    }
    if (cached[0]==0) cached[0] = DB.NULL;
  }

  protected long fileAttribute(long key, long value, long newAttribute, boolean write)
  {
    KeyFile header = getData(key, false);
    if (header==null) return DB.NULL;
    int base = header.base(key);
    int end = (int)header.read(base+DynamicMap.pLength)-1;
    if (end==-1) return DB.NULL;
    int valueBase = (int)header.read(base+DynamicMap.pSlotFilePosition);

    ValueFile file = fileManager.getValueFile(mapGetter, header.read(base+DynamicMap.pSlotFileId));

    int start = 0;
    int test;
    long currentValue;

    while (start<end)
    {
      test = (end + start) / 2;
      if (test % 2 !=0) test-=1;

      currentValue = file.read(valueBase+test);

      if (currentValue == value)
      {
        long attribute = file.read(valueBase+test+1);
        if (!write) return attribute;

        if (DB.NULL == newAttribute && DB.NULL == attribute) return WRITTEN;
        if (type!=MULTISET && newAttribute == attribute) return WRITTEN;

        if (type!=MULTISET) file.write(valueBase+test+1, newAttribute);
        else if (attribute==DB.NULL) file.write(valueBase+test+1, newAttribute);
        else if (newAttribute==DB.NULL) file.write(valueBase+test+1, DB.NULL);
        else file.write(valueBase+test+1, attribute+newAttribute);

        if (isIndexed()) if (newAttribute == DB.NULL) indexer.unIndex(key, attribute);
        else indexer.index(key, newAttribute);

        return WRITTEN;
      }

      if (currentValue < value) start = test+2; else end = test;
    }

    return NOTWRITTEN;
  }

  @Override
  protected LongStream scanningQuery(long lowestValue, long highestValue)
  {
    return keys().filter(key ->
    {
      return values(key).anyMatch(value -> (value!= DB.NULL && value >= lowestValue && value<=highestValue));
    });
  }

  @Override
  protected LongStream scanningUnionQuery(long... values)
  {
    return keys().filter(key ->
    {
      for (long value: values)
        if (fileAttribute(key, value, DB.NULL, false)!=DB.NULL) return true;
      return false;
    });
  }

  /**
   * Range query by attributes for ATTRIBUTEDSET
   * Note that the Stream is not sorted!
   * @param lowestAttribute minimum
   * @param highestAttribute maximum
   * @return matching keys
   */
  public Stream<SetEntry> attributeQuery(long lowestAttribute, long highestAttribute)
  {
    return scanningAttributeQuery(lowestAttribute, highestAttribute);
  }

  protected Stream<SetEntry> scanningAttributeQuery(long lowestAttribute, long highestAttribute)
  {
    Stream.Builder<SetEntry> b =  Stream.builder();

    keys().forEach(key ->
    {
      entries(key).filter(entry ->
      {
        return (entry.attribute!= DB.NULL && entry.attribute >= lowestAttribute && entry.attribute<=highestAttribute);
      }).forEach(entry -> b.add(entry));
    });

    return b.build();
  }

  /**
   * Or-query by attributes for ATTRIBUTEDSET
   * Note that the Stream is not sorted!
   * @param attributes acceptable attributes
   * @return matching keys
   */
  public Stream<SetEntry> attributeUnionQuery(long... attributes)
  {
    return scanningAttributeUnionQuery(attributes);
  }

  protected Stream<SetEntry> scanningAttributeUnionQuery(long... attributes)
  {
    Arrays.sort(attributes);

    Stream.Builder<SetEntry> b =  Stream.builder();

    keys().forEach(key ->
    {
      entries(key).filter(entry ->
      {
        return (Arrays.binarySearch(attributes, entry.attribute)>=0);
      }).forEach(entry -> b.add(entry));
    });

    return b.build();
  }

  @Override
  public boolean isEmpty()
  {
    setCache.flushAll();
    return super.isEmpty();
  }

  @Override
  public long getCount()
  {
    setCache.flushAll();
    return super.getCount();
  }

  @Override
  public LongStream keys(long lowestKey, long highestKey)
  {
    setCache.flushAll();
    return super.keys(lowestKey, highestKey);
  }

  @Override
  public LongStream keyset()
  {
    setCache.flushAll();
    return super.keyset();
  }

  @Override
  public boolean containsKey(long key)
  {
    setCache.flushAll();
    return super.containsKey(key);
  }

  @Override
  public boolean reserveKey(long key)
  {
    setCache.flushAll();
    return super.reserveKey(key);
  }

  @Override
  public void clear()
  {
    setCache.clear();
    super.clear();
  }

  @Override
  public LongStream query(long lowestValue, long highestValue)
  {
    setCache.flushAll();
    return super.query(lowestValue, highestValue);
  }

  @Override
  public LongStream unionQuery(long... values)
  {
    setCache.flushAll();
    return super.unionQuery(values);
  }

  protected static class SetCache
  {
    protected final SetMap setMap;
    protected final long[][] memory;
    protected int nextAddress = 0;

    protected final Collection<SetCache> caches = new ArrayList<>();
    private final long[][] keyMap;

    public SetCache(SetMap setMap, int MAXCACHEDSETSIZE)
    {
      this.setMap = setMap;
      caches.add(this);
      memory = new long[MAXCACHEDSETSIZE][];
      keyMap = new long[MAXCACHEDSETSIZE][21];
    }

    public void clear()
    {
      Arrays.fill(keyMap, 0);
    }

    public long[] get(final long key)
    {
      long[] hashedKeys = keyMap[(int) (key % keyMap.length)];

      for (int i = 1; i<hashedKeys[0]; i+=2)
        if (hashedKeys[i]==key) return memory[(int) (hashedKeys[i+1])];
      return null;
    }

    public void put(final long key, final long[] value)
    {
      long[] hashedKeys = keyMap[(int) (key % keyMap.length)];
      for (int i = 1; i<hashedKeys[0]; i+=2)
        if (hashedKeys[i]==key)
        {
          memory[(int) (hashedKeys[i+1])] = value;
          return;
        }
      if (nextAddress == memory.length) flushAll();
      else if (hashedKeys[0]==20) flush(key);
      hashedKeys[(int) hashedKeys[0]+1] = key;
      hashedKeys[(int) hashedKeys[0]+2] = nextAddress;
      memory[nextAddress] = value;
      hashedKeys[0]+=2;
      nextAddress++;
    }

    public void flush(long key)
    {
      long[] hashedKeys = keyMap[(int) (key % keyMap.length)];
      for (int i = 1; i<hashedKeys[0]; i+=2)
      {
        setMap.writeCached(hashedKeys[i], memory[(int) (hashedKeys[i+1])]);
        memory[(int) (hashedKeys[i+1])] = null;
      }
      hashedKeys[0] = 0;
    }

    public void flushAll()
    {
      if (needsFlushing()) for (int i = 0; i<keyMap.length; i++) flush(i);
      nextAddress = 0;
    }

    protected boolean needsFlushing()
    {
      return nextAddress > 0;
    }

    public void remove(final long key)
    {
      long[] hashedKeys = keyMap[(int) (key % keyMap.length)];
      for (int i = 1; i<hashedKeys[0]; i+=2)
        if (hashedKeys[i]==key)
        {
          //TODO not working?
          memory[(int) (hashedKeys[i]+1)] = null;
          hashedKeys[i] = -1;
          return;
        }
    }
  }

}
