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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.stremebase.file.KeyFile;
import com.stremebase.file.ValueFile;
import com.stremebase.file.FileManager.ValueSlot;


/**
 * The bridge between the map api and the low level file storage
 * <p>
 * Strictly for internal use only.
 * @author olli
 */
public class MapGetter
{
  protected final StremeMap map;

  MapGetter(StremeMap map)
  {
    this.map = map;
  }

  public StremeMap map()
  {
    return map;
  }

  public long getNodeSize()
  {
    return map.nodeSize;
  }

  public long getLargestValueFileId()
  {
    return map.largestValueFileId;
  }

  public long getNextValueFileId()
  {
    if (map.largestValueFileId==DB.NULL) map.largestValueFileId = 0;
    return ++map.largestValueFileId;
  }

  public TreeMap<Long, KeyFile> getKeyFiles()
  {
    return map.keyFiles;
  }

  public Map<Long, ValueFile> getValueFiles()
  {
    return ((DynamicMap)map).valueFiles;
  }

  public TreeMap<Long, List<ValueSlot>> getFreeValueSlots()
  {
    return map.getFreeValueSlots();
  }

  public long getKeysToaKeyFile()
  {
    return map.keysToAKeyFile;
  }
}
