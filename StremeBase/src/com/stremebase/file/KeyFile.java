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

import com.stremebase.base.DB;

/**
 * A buffer for storing data for keys
 * For internal use only.
 */
public class KeyFile extends DbFile
{
  public final long fromKey;
  protected final long nodeSize;
  protected final long keysToAKeyFile;
  protected long keySize = DB.NULL;

  protected KeyFile(long id, String fileName, long nodeSize, long keysToAKeyFile, boolean persisted)
  {
    super(id, fileName, keysToAKeyFile * nodeSize +1, persisted);
    this.nodeSize = nodeSize;
    this.keysToAKeyFile = keysToAKeyFile;
    this.fromKey = id<0 ? (id+1) * keysToAKeyFile : (id-1) * keysToAKeyFile;
  }

  public static long fileId(long key, long keysToAKeyFile)
  {
    long fileId = key / keysToAKeyFile;
    if (key>=0) fileId++;
    return fileId;
  }

  public int base(long key)
  {
    if (key<0) key = -key;
    return (int) ((key % keysToAKeyFile)*nodeSize+1);
  }

  public boolean setActive(int base, boolean active)
  {
    boolean state = read(base)==1;
    if (state == active) return false;
    if (active)
    {
      write(base, 1);
      changeSize(1);
    }
    else
    {
      write(base, 0);
      changeSize(-1);
    }
    return true;
  }

  public long size()
  {
    if (keySize==DB.NULL) keySize = read(0);
    return keySize;
  }

  private void changeSize(long amount)
  {
    if (keySize==DB.NULL) keySize = read(0);
    keySize+=amount;
  }

  @Override
  protected void commit()
  {
    if (keySize!=DB.NULL) write(0, keySize);
    super.commit();
  }
}
