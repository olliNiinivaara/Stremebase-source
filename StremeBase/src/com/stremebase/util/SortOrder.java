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

package com.stremebase.util;

import java.util.ArrayList;
import java.util.List;

import com.stremebase.util.LongArrays.LongComparator;

/**
 * A LongComparator that enables sorting by multiple criteria
 * @author olli
 *
 */
public class SortOrder implements LongComparator
{
  protected List<LongComparator> singleComparators = new ArrayList<>();

  public boolean reversed;

  public SortOrder add(LongComparator longComparator)
  {
    singleComparators.add(longComparator);
    return this;
  }

  public void clear()
  {
    singleComparators.clear();
  }

  @Override
  public long compare(long v, long w)
  {
    for (LongComparator comparator : singleComparators)
    {
      long result = reversed ?  comparator.compare(w, v) : comparator.compare(v, w);

      if (result != 0) return result;
    }
    return 0;
  }

  @Override
  public boolean equals(Object object)
  {
    return object != null && object instanceof SortOrder  && singleComparators.equals(((SortOrder)object).singleComparators);
  }
}
