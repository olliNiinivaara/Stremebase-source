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

import java.util.Arrays;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.stremebase.base.DB;
import com.stremebase.base.DynamicMap;
import com.stremebase.base.util.SetCache;
import com.stremebase.file.FileManager.ValueSlot;
import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;


/**
 * SetMap associates a key with a set or a bag of values.
 * Set the set type in constructor:
 * SetMap.SET: An ordinary set
 * SetMap.MULTISET: A bag - occurrences are counted
 * SetMap.ATTRIBUTEDSET: Values can be further associated with an arbitrary tag (for example type or weight of value) 
 */
public class SetMap extends DynamicMap
{
  public static final byte SET = 0;
  public static final byte MULTISET = 1;
  public static final byte ATTRIBUTEDSET = 2;

  protected final byte type;
  protected final SetCache setCache;
  protected final long[] overwriterCache = new long[2+DB.db.MAXCACHEDSETVALUEENTRIES*2];

  private static final long NOTWRITTEN = DB.NULL;
  private static final long WRITTEN = 0;

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

    protected SetEntry(long key, long value, long attribute)
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

  /**
   * Creates a new SetMap with type SET for associating many values with each key.
   * The returned map is persistent iff the database is.
   *
   * @param mapName
   *          name for the map. Must be a database-wide unique value.
   */
  public SetMap(String mapName)
  {
    super(mapName, DB.db.INITIALCAPACITY, DB.isPersisted());
    this.type = SET;
    setCache = new SetCache(this);
  }

  /**
   * Creates a new SetMap of given type
   * @param mapName the name
   * @param initialCapacity Initial capacity of each set
   * @param type the type (SET, MULTISET or ATTRIBUTEDSET)
   * @param persist if you persist
   */
  public SetMap(String mapName, int initialCapacity, byte type, boolean persist)
  {
    super(mapName, initialCapacity, persist);
    this.type = type;
    setCache = new SetCache(this);
  }

  @Override
  public void commit()
  {
    setCache.commitAll();
    super.commit();
  }

  @Override
  public void addIndex(byte indexType)
  {
    if (indexType == DB.MANY_TO_MULTIMANY)
      throw new IllegalArgumentException("SetMap does not support DB.MANY_TO_MULTIMANY indexing.");
    super.addIndex(indexType);
  }

  @Override
  public void reIndex()
  {
    indexer.clear();
    commit();
    keys().forEach(key -> (values(key).forEach(value -> (indexer.index(key, value)))));
    indexer.commit();
  }

  /**
   * Returns the size of the set, including DB.NULL values
   */
  public long getSize(long key)
  {
    flush(key, setCache.get(key));
    return super.getSize(key)/2;
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
    long[] set = setCache.get(key);
    if (set!=null) writeCached(key, set);

    final byte[] index = new byte[1];
    index[0] = 0;
    final long[] entry = new long[2];

    return super.values(key, false, false).filter(value ->
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
    long[] set = setCache.get(key);
    if (set!=null) writeCached(key, set);

    final boolean[] isValue = new boolean[1];
    isValue[0] = true;
    final long[] theValue = new long[1];

    return super.values(key, false, false).filter(value ->
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

  protected void put(long key, long value, long attribute)
  {
    if (key<0) throw new IllegalArgumentException("Negative keys are not supported ("+key+")");
    if (value==DB.NULL) throw new IllegalArgumentException("Value cannot be DB.NULL");

    long[] set = setCache.get(key);
    if (set==null)
    {
      set = new long[2+DB.db.MAXCACHEDSETVALUEENTRIES*2];
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
      if (set[0]+2==set.length) setCache.commit(key);
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
    if (set[0]+2==set.length) setCache.commit(key);
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
   * @param value thr value
   */
  public void remove(long key, long value)
  {
    if (type == MULTISET) put(key, value, -1); else put(key, value, DB.NULL);
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

  /**
   * Writes cache to buffer.
   * Used only internally.
   * @param key the key
   * @param value the cached part
   */
  public void flush(long key, long[] value)
  {
    if (value == null) return;
    if (value[0]==0) return;
    writeCached(key, value);
  }

  protected void writeCached(long key, long[] cached)
  {
    KeyFile header = getData(key, false);

    if (header!=null)
    {
      overWrite(key, cached);
      if (cached[0] == DB.NULL) return;
    }

    header = getData(key, true);
    int base = header.base(key);
    final long oldLength = super.getSize(key);
    header.setActive(base, true);

    final ValueSlot newSlot = DB.fileManager.getFreeSlot(mapGetter, cached[0]+oldLength+2);

    long newPos = newSlot.slotPosition;

    int cachePos = 2;
    long newLength = 0;

    if (oldLength>0)
    {
      ListIterator li = new ListIterator(key, oldLength, false, false);
      while (li.hasNext())
      {
        long oldValue = li.nextLong();
        li.hasNext();
        long oldAttribute = li.nextLong();

        if (cachePos>=cached[0]+2 || (cachePos<cached[0]+2 && oldValue<cached[cachePos]))
        {
          if (oldAttribute!=DB.NULL)
          {
            newSlot.valueFile.write(newPos, oldValue);
            newSlot.valueFile.write(newPos+1, oldAttribute);
            newPos+=2;
            newLength++;
          }
        }
        else
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

    ValueFile file = DB.fileManager.getValueFile(mapGetter, header.read(base+DynamicMap.pSlotFileId));

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

        if (isIndexed()) if (newAttribute == DB.NULL) indexer.remove(key, attribute);
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
}
