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


import java.util.Spliterator;
import java.util.Stack;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import com.stremebase.map.StackListMap;


/**
 * Converts strings to longs and back.
 * <p>
 * Used internally
 * <p>
 * High-level access via {@link com.stremebase.dal.Value}
 * @author olli
 */
public class Lexicon
{
  protected static final int width = 30*2+2;

  protected StackListMap strings;
  protected final StringBuffer bufs = new StringBuffer();

  public Lexicon(DB db)
  {
    db.defineMap("Stremebase_lexicon", StackListMap.class, db.props().add("INITIALCAPACITY", width).build(), true);
    strings = db.getMap("Stremebase_lexicon");
  }

  public void commit()
  {
    strings.flush();
  }

  public long[] useText(String sentence, String splitter, boolean put)
  {
    return useWords(put, sentence.split(splitter));
  }

  public long[] useWords(boolean put, CharSequence... words)
  {
    long[] result = new long[words.length];
    for (int i=0; i<result.length; i++) result[i] = useWord(words[i], put);
    return result;
  }

  public long useWord(CharSequence word, boolean put)
  {
    if (word.length()==0)
    {
      if (!put) return DB.NULL;
      throw new IllegalArgumentException("Puttin' on empty string");
    }

    if (word.length()==1) return word.charAt(0);

    int key = word.charAt(0);

    outerloop: for (int c = 1; c<word.length(); c++)
    {
      int i = 0;
      while (true)
      {
        long existing = get(key, i+2);

        if (existing==DB.NULL)
        {
          if (!put) return DB.NULL;
          put(key, i+2, word.charAt(c));
          final int nextPosition = addChar(word.charAt(c), key, c==word.length()-1);
          put(key, i+3, nextPosition);
          key = nextPosition;
          continue outerloop;
        }
        else if (existing==word.charAt(c))
        {
          key = (int)get(key, i+3);
          if (put && c==word.length()-1) put(key, 0, (int)existing);
          continue outerloop;
        }
        i+=2;
      }
    }

    return key;
  }

  protected int addChar(int chr, int previouscharKey, boolean completeWord)
  {
    long key = getNextKey();
    if (key<=Character.MAX_VALUE) key = Character.MAX_VALUE+1;
    if (!completeWord) chr = -chr;
    put(key, 0, chr);
    put(key, 1, previouscharKey);
    return (int)key;
  }

  public long getIfWord(CharSequence word)
  {
    long key = useWord(word, false);
    if (key==DB.NULL) return DB.NULL;
    if (get(key, 0)<=0) return DB.NULL;
    return key;
  }

  public void getWord(long key, StringBuilder string)
  {
    string.setLength(0);

    if (key<0)
    {
      string.append("DB.NULL");
      return;
    }

    if (key > Character.MAX_VALUE && get(key, 0)<0) return;

    while (key > Character.MAX_VALUE)
    {
      string.append((char)(Math.abs(get(key, 0))));
      key = get(key, 1);
    }
    string.append((char)key);
    string.reverse();
  }

  public LongStream wordsWithPrefix(CharSequence prefix)
  {
    long key = useWord(prefix, false);
    if (key==DB.NULL) return LongStream.empty();
    return StreamSupport.longStream(new WordSpliterator(key), false);
  }

  protected long get(long key, int index)
  {
    return strings.get(key, index);
  }

  protected void put(long key, int index, long value)
  {
    strings.put(key, index, value);
  }

  protected long getNextKey()
  {
    return strings.getLargestKey()+1;
  }

  private class WordSpliterator implements Spliterator.OfLong
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
          final long existing = get(key, i+2);
          if (existing==0 || existing == DB.NULL) break;
          stack.push(get(key, i+3));
          i+=2;
        }

        if (get(key, 0)>0)
        {
          action.accept(key);
          return true;
        }
      }
    }
  }
}
