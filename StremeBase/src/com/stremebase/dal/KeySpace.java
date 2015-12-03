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

package com.stremebase.dal;

/**
 * Shares the Long.MAX_VALUE key space between tables with even-sized partitions so that relations and search results can contain separable keys from multiple tables
 * @author olli
 *
 */
public class KeySpace
{
  /**
   * The shared key class
   * @author olli
   *
   */
  public static class SharedKey
  {
    final public Table table;
    final public long key;
    final public long globalKey;

    public SharedKey(Table table, long key)
    {
      this.table = table;
      this.key = key;
      this.globalKey = getGlobalKey(table, key);
    }

    public SharedKey(long globalKey)
    {
      this.globalKey = globalKey;
      this.table = Table.getTable(getTableId(globalKey));
      this.key = getLocalKey(globalKey);
    }
  }

  /**
   * How the key space is partitioned. Cannot be changed afterwards. Default value 10000 allows 10000 tables and 922337200000000 keys per table.
   */
  public static final int spaceSize = 10000;
  public static final long spaceSlot = Long.MAX_VALUE / spaceSize;

  public static long getGlobalKey(Table table, long localKey)
  {
    if (localKey>=spaceSlot) throw new IllegalArgumentException(localKey+" > max. key size "+(spaceSlot-1));
    return table.id * spaceSlot + localKey;
  }

  public static long getLocalKey(long globalKey)
  {
    return globalKey % spaceSlot;
  }

  public static int getTableId(long globalKey)
  {
    return (int) (globalKey / spaceSlot);
  }

  public static long lowestGlobalKey(Table table)
  {
    return table.id * spaceSlot;
  }

  public static long highestGlobalKey(Table table)
  {
    return ((1+table.id) * spaceSlot) - 1;
  }
}
