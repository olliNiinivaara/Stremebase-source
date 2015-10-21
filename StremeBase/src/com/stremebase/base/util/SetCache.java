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

package com.stremebase.base.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.ArrayList;

import com.stremebase.base.DB;
import com.stremebase.map.SetMap;

/**
 * Used internally by SetMap
 */
public class SetCache
{
  protected final SetMap setMap;
  protected final long[][] memory = new long[DB.db.MAXCACHEDSETSIZE][];
  protected int nextAddress = 0;

  protected static final Collection<SetCache> caches = new ArrayList<>();

  private final long[][] keyMap = new long[DB.db.MAXCACHEDSETSIZE][21];

  public SetCache(SetMap setMap)
  {
    this.setMap = setMap;
    caches.add(this);
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
    if (nextAddress == memory.length) commitAll();
    else if (hashedKeys[0]==20) commit(key);
    hashedKeys[(int) hashedKeys[0]+1] = key;
    hashedKeys[(int) hashedKeys[0]+2] = nextAddress;
    memory[nextAddress] = value;
    hashedKeys[0]+=2;
    nextAddress++;
  }

  public void commit(long key)
  {
    long[] hashedKeys = keyMap[(int) (key % keyMap.length)];
    for (int i = 1; i<hashedKeys[0]; i+=2)
    {
      setMap.flush(hashedKeys[i], memory[(int) (hashedKeys[i+1])]);
      memory[(int) (hashedKeys[i+1])] = null;
    }
    hashedKeys[0] = 0;
  }

  public void commitAll()
  {
    for (int i = 0; i<keyMap.length; i++) commit(i);
    nextAddress = 0;
  }

  public void remove(final long key)
  {
    long[] hashedKeys = keyMap[(int) (key % keyMap.length)];
    for (int i = 1; i<hashedKeys[0]; i+=2)
      if (hashedKeys[i]==key)
      {
        memory[(int) (hashedKeys[i]+1)] = null;
        hashedKeys[i] = -1;
        return;
      }
  }
}
