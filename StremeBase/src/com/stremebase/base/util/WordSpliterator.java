package com.stremebase.base.util;

import java.util.Spliterator;
import java.util.Stack;
import java.util.function.LongConsumer;

import com.stremebase.base.DB;

class WordSpliterator implements Spliterator.OfLong
{     
  protected final Stack<Long> stack = new Stack<>();
  
  protected WordSpliterator(long key)
  {
    stack.clear();
    stack.push(key);
  }
  
  @Override
  public long estimateSize()
  {
    return Long.MAX_VALUE;
  }

  @Override
  public int characteristics()
  {
    return DISTINCT | IMMUTABLE | NONNULL;
  }

  @Override
  public java.util.Spliterator.OfLong trySplit()
  {
    return null;
  }
  
  @Override
  public boolean tryAdvance(LongConsumer action)
  {
    while (true)
    {
      if (stack.isEmpty()) return false;
      long key = stack.pop();
      int i = 0;
      while (true)
      {
        final long existing = StrToInt.strings.get(key, i+2);
        if (existing==0 || existing == DB.NULL) break;
        stack.push(StrToInt.strings.get(key, i+3));
        i+=2;
      }

      if (StrToInt.strings.get(key, 0)>0)
      {
        action.accept(key);
        return true;
      }
    }
  }
} 