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

package com.stremebase.file;


import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.stremebase.base.DB;
import com.stremebase.base.FixedMap;
import com.stremebase.base.MapGetter;


public class FileManager
{		
	public static MapGetter loadingProperty;
	public static volatile int cachedFreeSlots = 0;
	
	protected static final Map<String, MapGetter> loadedMaps = new HashMap<>();
	
	public class ValueSlot implements Serializable
	{
		private static final long serialVersionUID = 2497223089728027757L;
				
		public ValueFile valueFile;
		public long slotPosition;
		public long slotSize;
		
		ValueSlot(ValueFile valueFile, long slotPosition, long slotSize)
		{
			this.valueFile = valueFile;
			this.slotPosition = slotPosition;	
			this.slotSize = slotSize;
		}
		
		private void writeObject(ObjectOutputStream o) throws IOException
		{    
			o.writeLong(valueFile.id);
			o.writeLong(slotPosition);
			o.writeLong(slotSize);
		}
			  
		private void readObject(ObjectInputStream o) throws IOException, ClassNotFoundException
		{  	    
			this.valueFile = DB.fileManager.getValueFile(loadingProperty, o.readLong());
			this.slotPosition = o.readLong();	
			this.slotSize = o.readLong();
		}
	}
	
	public static void deleteDir(File dir)
  {
    if (dir==null || !dir.exists()) return;
    File[] files = dir.listFiles();
    for(File f: files) if(f.isDirectory()) deleteDir(f); else f.delete();
    dir.delete();
  }	
	
	public String getDirectory(MapGetter pd, char type)
	{
		String dir = DB.db.DIRECTORY+pd.map().getMapName()+File.separatorChar+type+File.separatorChar;
		if (DB.Persisted())
		{
		  File f = new File(dir);
		  if (!f.exists()) f.mkdirs();
		}
		return dir;
	}
	
	public long loadProperty(MapGetter pd)
	{
		if (loadedMaps.containsKey(pd.map().getMapName())) throw new IllegalArgumentException("Map "+pd.map().getMapName()+" is already loaded"); 
		loadedMaps.put(pd.map().getMapName(), pd);	
		if (pd.map().isPersisted())
		{
		  loadKeyFiles(pd);
		  if (!(pd.map() instanceof FixedMap)) return loadValueFiles(pd);
		}
    return DB.NULL;
	}
	
	public void commit(MapGetter property)
	{		
		TreeMap<Long, KeyFile> files =  property.getKeyFiles();
		for (KeyFile file: files.values()) file.commit();
		
		Map<Long, ValueFile> vFiles = property.getValueFiles();
		for (DbFile file: vFiles.values()) file.commit();
	}
	
	public void clear(MapGetter property)
	{
		TreeMap<Long, KeyFile> kFiles = property.getKeyFiles();
		for (KeyFile kF: kFiles.values()) kF.delete();
		kFiles.clear();
		//new File(getDirectory(property, 'K')).delete();
		
		Map<Long, ValueFile> vFiles = property.getValueFiles();
		for (ValueFile vF: vFiles.values()) vF.delete();
		vFiles.clear();
		//new File(getDirectory(property, 'V')).delete();
		
		TreeMap<Long, List<ValueSlot>> slots = property.getFreeValueSlots();
		if (slots!=null)
		{
		  for (List<ValueSlot> slotBag: slots.values()) cachedFreeSlots-=slotBag.size();
		  slots.clear();
		}
		
		if (property.map().isPersisted()) deleteDir(new File(DB.db.DIRECTORY+property.map().getMapName()));
	}
	
	public void close(MapGetter map)
	{
		loadedMaps.remove(map.map().getMapName());
	}
	
	public void commitAll()
	{
		for (MapGetter md: loadedMaps.values()) commit(md);
	}
	
	public void closeAll()
	{
		loadedMaps.clear();
	}
	
	public void clearAll()
	{
		for (MapGetter md: loadedMaps.values()) clear(md);
	}
		
	protected void loadKeyFiles(MapGetter pd)
	{
		TreeMap<Long, KeyFile> files = pd.getKeyFiles();
		String dir = getDirectory(pd, 'K');
		File[] fileList = new File(dir).listFiles();
		if (fileList ==null) return;
		for (File f: fileList)
		{
			long id = Long.parseLong(f.getName().substring(2, f.getName().length()-3));
			KeyFile file = new KeyFile(id, f.getAbsolutePath(), pd.getNodeSize(), (pd.map().isPersisted()));
			files.put(id, file);
		}
	}
	
	protected long loadValueFiles(MapGetter pd)
	{
		Map<Long, ValueFile> files = pd.getValueFiles();
		String dir = getDirectory(pd, 'V');
		File[] fileList = new File(dir).listFiles();
		if (fileList ==null) return DB.NULL;
		
		long largestId = DB.NULL;
		
		for (File f: fileList)
		{
			long id = Long.parseLong(f.getName().substring(2, f.getName().length()-3));
			if (id>largestId) largestId = id;
			ValueFile file = new ValueFile(id, f.getAbsolutePath(), DB.NULL, (pd.map().isPersisted()));					
			files.put(id, file);
		}
		
		return largestId;
	}
			
	public KeyFile getNextKeyFile(MapGetter property, long fileId)
	{				
		Long nextId = property.getKeyFiles().ceilingKey(fileId+1);
    if (nextId==null) return null;
  	return getKeyFile(property, nextId, DB.NULL);
	}
	
	public KeyFile getKeyFile(MapGetter property, long fileId, long nodeSize)
	{		
		KeyFile result = property.getKeyFiles().get(fileId);
		if (nodeSize!=DB.NULL && result == null)
		{
			String fileName = getDirectory(property, 'K')+"db"+fileId+".db";
			result = new KeyFile(fileId, fileName, nodeSize, property.map().isPersisted());	
			property.getKeyFiles().put(fileId, result);
		}
		return result;
	}
			
	public ValueFile getValueFile(MapGetter property, long fileId)
	{
		return property.getValueFiles().get(fileId);
	}
	
	public ValueSlot getFreeSlot(MapGetter property, long requiredSize)
	{
		ValueSlot slot = findSlot(property, requiredSize);
		if (slot!=null) return slot;

		long fileId = property.getLargestValueFileId();
		ValueFile file;
		if (fileId==DB.NULL) file = createNewValueFile(property, requiredSize);
		else
		{
		  file = getValueFile(property, fileId);
		  if (file.getRemainingCapacity()<requiredSize)
		  {
		  	System.out.printf("luotiin uusi valuefile: tarve oli %d mutta jäljellä oli vain %d%n", requiredSize, file.getRemainingCapacity());
		  	//TODO vapautetaanko kahesti (myös primitivelistmapista?)
		  	releaseSlot(property, fileId, file.getRemainingCapacity(), file.getCapacity() -  file.getRemainingCapacity());
		  	file = createNewValueFile(property, requiredSize);
		  }
		}
		return new ValueSlot(file, file.getAndSetEof(requiredSize), requiredSize);
	}
	
	protected ValueFile createNewValueFile(MapGetter property, long requiredSize)
	{
		long fileId = property.getNextValueFileId();
		String fileName = getDirectory(property, 'V')+"db"+fileId+".db";
		if (requiredSize < DB.db.INITIALVALUEFILESIZE<<(fileId-1)) requiredSize = DB.db.INITIALVALUEFILESIZE<<(fileId-1);
		ValueFile file = new ValueFile(fileId, fileName, requiredSize, property.map().isPersisted());		
		Map<Long, ValueFile> files = property.getValueFiles();
	  files.put(fileId, file);
	  return file;
	}
	
	protected ValueSlot findSlot(MapGetter property, long requiredSize)
	{
		TreeMap<Long, List<ValueSlot>> slots = property.getFreeValueSlots();
		
		Long slotSize = slots.ceilingKey(requiredSize);
   	if (slotSize == null) return null;
   	List<ValueSlot> slotBag = slots.get(slotSize);
		if (slotBag==null || slotBag.size() == 0) return null;
		return slotBag.remove(slotBag.size()-1);
	}
	
	public void releaseSlot(MapGetter property, long fileId, long slotSize, long slotPosition)
	{		
		TreeMap<Long, List<ValueSlot>> slots = property.getFreeValueSlots();
      	
   	List<ValueSlot> slotBag;
   	Long position = slots.ceilingKey(slotSize);
   	if (position == null)
   	{
   		position = slotSize;
   		slotBag = new ArrayList<ValueSlot>();
			slots.put(position, slotBag);
   	}
   	else
   	{
   		slotBag = slots.get(position);
   		if (cachedFreeSlots >= DB.db.MAXCACHEDFREESLOTS)
   		{
   			Long smallest = slots.firstKey();
   		  if (position == smallest) return;
   	 	  cachedFreeSlots -= slots.get(smallest).size();
   		  slots.remove(smallest);
   		}
   	}
		
   	slotBag.add(new ValueSlot(getValueFile(property, fileId), slotPosition, slotSize));
   	cachedFreeSlots++;
	}
}