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

/**
 * A buffer for storing value slots
 * For internal use only.
 */
public class ValueFile extends DbFile
{
  protected static final int pEof = 0;

  protected ValueFile(long id, String fileName, long requiredSize, boolean persisted)
  {
    super(id, fileName, requiredSize, persisted);
  }

  protected long getRemainingCapacity()
  {
    long eof = read(pEof);
    if (eof==0) write(pEof, 1);
    return getCapacity() - eof;
  }

  protected long getAndSetEof(long amount)
  {
    long eof = read(pEof);
    if (eof==0) eof = 1;
    write(pEof, eof+amount);
    return eof;
  }
}