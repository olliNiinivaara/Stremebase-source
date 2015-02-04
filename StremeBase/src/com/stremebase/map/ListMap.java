package com.stremebase.map;

import com.stremebase.base.DynamicMap;

public class ListMap extends DynamicMap
{
  public ListMap(String mapName, int minimumSize, int indexType, boolean persist)
  {
    super(mapName, minimumSize, indexType, persist);
  }
  
  @Override
  public void put(long key, int index, long value)
  {
    super.put(key, index, value);
  }
  
  @Override
  public long get(long key, int index)
  {
    return super.get(key, index);
  }
}
