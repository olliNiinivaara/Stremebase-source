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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;
import com.stremebase.file.FileManager.ValueSlot;
import com.stremebase.map.StackListMap;

/**
 * Abstract base class for maps that grow their value storages as needed (lists, sets, etc.)
 * @author olli
 */
public abstract class DynamicMap extends StremeMap
{
  protected static final int pLength = 1;
  protected static final int pSlotSize = 2;
  protected static final int pSlotFileId = 3;
  protected static final int pSlotFilePosition = 4;
  protected int initialCapacity;

  protected TreeMap<Long, List<ValueSlot>> freeValueSlots;

  @Override
  public void initialize(String mapName, Catalog catalog)
  {
    catalog.setProperty(Catalog.NODESIZE, this, 5);
    super.initialize(mapName, catalog);
    initialCapacity = (int) catalog.getProperty(Catalog.INITIALCAPACITY, this);
  }

  @Override
  public void close()
  {
    super.close();
    if (!persisted || freeValueSlots == null) return;
    FileOutputStream fos;
    String free = fileManager.getDirectory(mapGetter, 'F', true)+"free.ser";
    try
    {
      fos = new FileOutputStream(free);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeObject(freeValueSlots);
      oos.close();
    }
    catch (Exception e)
    {
      System.out.println("Could not serialize free slots: "+e.getMessage());
      new File(free).delete();
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected TreeMap<Long, List<ValueSlot>> getFreeValueSlots()
  {
    if (freeValueSlots!=null) return freeValueSlots;

    if (!persisted)
    {
      freeValueSlots = new TreeMap<Long, List<ValueSlot>>();
      return freeValueSlots;
    }

    fileManager.loadingMap = mapGetter;
    File free = new File(fileManager.getDirectory(mapGetter, 'F', false)+"free.ser");
    if (!free.exists())
    {
      freeValueSlots = new TreeMap<Long, List<ValueSlot>>();
      return freeValueSlots;
    }

    try
    {
      FileInputStream in = new FileInputStream(free);
      ObjectInputStream ois = new ObjectInputStream(in);
      fileManager.loadingMap=mapGetter;
      freeValueSlots = (TreeMap<Long, List<ValueSlot>>) ois.readObject();
      ois.close();
      free.delete();
      for (List<ValueSlot> slotBag: freeValueSlots.values()) fileManager.cachedFreeSlots+=slotBag.size();
    }
    catch (Exception e)
    {
      e.printStackTrace();
      if (freeValueSlots==null) freeValueSlots = new TreeMap<Long, List<ValueSlot>>();
    }
    return freeValueSlots;
  }

  @Override
  public long getValueCount(long key)
  {
    if (key==DB.NULL) return 0;
    KeyFile header = getData(key, false);
    if (header==null) return 0;
    return (int) header.read(header.base(key)+pLength);
  }

  @Override
  protected void put(long key, int index, long value)
  {
    if (key<0) throw new IllegalArgumentException("Negative keys are not supported ("+key+")");
    if (index<-1) throw new IndexOutOfBoundsException("Index out of bounds: "+index);

    ValueFile slot;
    KeyFile header = getData(key, true);
    int base = header.base(key);
    if (header.read(base) == 0)
    {
      long[] newList = new long[index+1];
      newList[index] = value;
      putToNewSlot(key, newList);
      if (isIndexed() && (!(this instanceof StackListMap && index==0))) if (value!=DB.NULL) indexer.index(key, value);
      header.setActive(base, true);
      return;
    }

    int length = (int)header.read(base+pLength);
    if (index == -1 || index>length) index = length;
    if (index==length) header.write(base+pLength, length+1);

    slot = getSlot(key);

    boolean fits = header.read(base+pSlotSize)>=index+1;

    long oldValue = DB.NULL;

    if (fits)
    {
      if (index<length) oldValue = slot.read(position+index);
      slot.write(position+index, value);
    }
    else
    {
      long[] newList = new long[length+1];
      //TODO stream?
      get(key, newList);
      newList[length] = value;
      ValueSlot fs = putToNewSlot(key, newList);
      slot = fs.valueFile;
      position = (int)fs.slotPosition;
    }

    if (oldValue!=value && isIndexed())
    {
      if (oldValue!=DB.NULL) indexer.unIndex(key, oldValue);
      if (value!=DB.NULL) indexer.index(key, value);
    }
  }

  protected void put(long key, int index, long... values)
  {
    for (int i = 0; i<values.length; i++) put(key, index+i, values[i]);
  }

  private int streamIndex;

  protected void put(long key, int index, LongStream values)
  {
    streamIndex = 0;

    //TODO batch job?
    values.sequential().forEach(value ->  put(key, index+streamIndex++, value));
  }

  @Override
  public void remove(long key)
  {
    KeyFile header = getData(key, false);
    if (header==null) return;
    if (isIndexed()) values(key).forEach(value -> indexer.unIndex(key, value));
    header.setActive(header.base(key), false);
    ValueFile slot = getSlot(key);
    if (slot!=null) releaseSlot(key);
  }

  protected int indexOf(long key, int fromIndex, long value)
  {
    if (fromIndex<0) throw new IndexOutOfBoundsException("Index out of bounds: "+fromIndex);

    KeyFile header = getData(key, false);
    if (header==null) return -1;
    int base = header.base(key);
    if (header.read(base)==0) return -1;
    ValueFile slot = getSlot(key);
    long length = header.read(base+pLength);
    for (int i = fromIndex; i < length; i++) if (slot.read(position+i)==value) return i;
    return -1;
  }

  protected long get(long key, int index)
  {
    if (index<0) throw new IndexOutOfBoundsException("Index out of bounds: "+index);
    ValueFile slot = getSlot(key);
    if (slot==null) return DB.NULL;
    KeyFile header = getData(key, false);
    long length = header.read(header.base(key)+pLength);
    if (index>=length) return DB.NULL;
    return slot.read(position+index);
  }

  protected void get(long key, long[] toArray)
  {
    KeyFile header = getData(key, false);
    if (header==null) throw new IndexOutOfBoundsException("Cannot get nonexistent list.");
    int base = header.base(key);
    if (header.read(base)==0) throw new IndexOutOfBoundsException("Cannot get nonexistent list.");
    ValueFile file = fileManager.getValueFile(mapGetter, header.read(base+pSlotFileId));
    int length = (int)header.read(base+pLength);
    if (length>toArray.length) length = toArray.length;
    file.readToArray((int)header.read(base+pSlotFilePosition), toArray, length);
  }

  protected boolean listEquals(long key, long[] list)
  {
    if (getValueCount(key)!=list.length) return false;
    long[] values = new long[list.length];
    get(key, values);
    return Arrays.equals(list, values);
  }

  protected LongStream values(long key, boolean skipNulls)
  {
    return StreamSupport.longStream(spliterator(key, skipNulls), false);
  }

  protected Spliterator.OfLong spliterator(long key, boolean skipNulls)
  {
    long length = 0;
    KeyFile header = getData(key, false);
    if (header==null) return Spliterators.emptyLongSpliterator();
    int base = header.base(key);
    if (header.read(base)==0) return Spliterators.emptyLongSpliterator();
    length = header.read(base+pLength);
    if (length==0) return Spliterators.emptyLongSpliterator();

    return Spliterators.spliterator(new ListIterator(key, length, skipNulls), length,
        java.util.Spliterator.IMMUTABLE | java.util.Spliterator.NONNULL | java.util.Spliterator.ORDERED);
  }

  protected void createHeader(long key, long length, final ValueSlot slotInfo)
  {
    KeyFile header = getData(key, true);
    int base = header.base(key);

    if (!header.setActive(base, true)) fileManager.releaseSlot(mapGetter, header.read(base+pSlotFileId), header.read(base+pSlotSize), header.read(base+pSlotFilePosition));

    header.write(base+pLength, length);
    header.write(base+pSlotSize, slotInfo.slotSize);
    header.write(base+pSlotFileId, slotInfo.valueFile.id);
    header.write(base+pSlotFilePosition, slotInfo.slotPosition);
  }

  protected void releaseSlot(long key)
  {
    KeyFile header = getData(key, false);
    int base = header.base(key);
    if (header.setActive(base, false)) fileManager.releaseSlot(mapGetter, header.read(base+pSlotFileId), header.read(base+pSlotSize), header.read(base+pSlotFilePosition));
  }

  protected ValueSlot putToNewSlot(long key, long[] values)
  {
    int slotSize = values.length<initialCapacity ? initialCapacity : values.length * 2;
    final ValueSlot slot = fileManager.getFreeSlot(mapGetter, slotSize);
    writeData(slot, values);
    createHeader(key, values.length, slot);
    return slot;
  }

  private int position;

  protected ValueFile getSlot(long key)
  {
    KeyFile header = getData(key, false);
    if (header==null) return null;
    int base = header.base(key);
    if (header.read(base)==0) return null;
    position = (int)header.read(base+pSlotFilePosition);
    return fileManager.getValueFile(mapGetter, header.read(base+pSlotFileId));
  }

  protected void writeData(ValueSlot slotInfo, long[] values)
  {
    for (int i=0; i<values.length; i++)
      slotInfo.valueFile.write((int)slotInfo.slotPosition+i, values[i]);
  }

  //---------------------------------------------------------------

  protected class ListIterator implements PrimitiveIterator.OfLong
  {
    final ValueFile slot;
    int remaining;
    int pos;
    long current = DB.NULL;
    final boolean skipNulls;

    public ListIterator(long key, long length, boolean skipNulls)
    {
      slot = getSlot(key);
      remaining = (int)length;
      pos = position;
      this.skipNulls = skipNulls;
    }

    @Override
    public void forEachRemaining(LongConsumer action)
    {
      while (hasNext()) action.accept(nextLong());
    }

    @Override
    public boolean hasNext()
    {
      if (remaining==0) return false;
      current = slot.read(pos++);
      if (skipNulls)
      {
        while (current==DB.NULL && remaining-->0) current = slot.read(pos++);
        if (current==DB.NULL) return false;
      }
      return remaining-->0;
    }

    @Override
    public long nextLong()
    {
      return current;
    }
  }
}
