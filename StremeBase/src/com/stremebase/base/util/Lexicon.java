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


import java.util.Spliterator;
import java.util.Stack;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import com.stremebase.base.DB;
import com.stremebase.map.ListMap;


public class Lexicon
{	
  protected static final int width = 30*2+2;

  protected static ListMap strings;
  protected static final StringBuffer bufs = new StringBuffer();

  private static final Lexicon instance = new Lexicon();
  private Lexicon() {}

  public static void initialize(boolean persist)
  {
    if (strings == null) strings = new ListMap("Stremebase_lexicon", width, DB.isPersisted());
  }

  public static void commit()
  {
    strings.commit();
  }

  public static long[] useText(String sentence, String splitter, boolean put)
  {
    return useWords(put, sentence.split(splitter));
  }

  public static long[] useWords(boolean put, CharSequence... words)
  {
    long[] result = new long[words.length];
    for (int i=0; i<result.length; i++) result[i] = useWord(words[i], put);
    return result;
  }

  synchronized public static long useWord(CharSequence word, boolean put)
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
        long existing = strings.get(key, i+2);

        if (existing==DB.NULL)	
        { 				
          if (!put) return DB.NULL;
          strings.put(key, i+2, word.charAt(c));
          final int nextPosition = addChar(word.charAt(c), key, c==word.length()-1);
          strings.put(key, i+3, nextPosition);
          key = nextPosition;
          continue outerloop;
        }
        else
        {  				
          if (existing==word.charAt(c))
          {
            key = (int)strings.get(key, i+3);
            if (put && c==word.length()-1) strings.put(key, 0, (int)existing);
            continue outerloop;
          }
        }
        i+=2;
      }
    }

    return key;
  }

  protected static int addChar(int chr, int previouscharKey, boolean completeWord)
  {
    long key = strings.getLargestKey()+1;
    if (key<=Character.MAX_VALUE) key = Character.MAX_VALUE+1;
    if (!completeWord) chr = -chr;
    strings.put(key, 0, chr);
    strings.put(key, 1, previouscharKey);		
    return (int)key;
  }

  public static long getIfExists(CharSequence word)
  {
    long key = useWord(word, false);
    if (key==DB.NULL) return key;
    if (strings.get(key, 0)>0) return key;
    return DB.NULL;
  }

  public static void getWord(long key, StringBuilder string)
  {
    string.setLength(0);

    if (key<0)
    {
      string.append("DB.NULL");
      return;
    }

    if (key > Character.MAX_VALUE && strings.get(key, 0)<0) return;

    while (key > Character.MAX_VALUE)
    {     
      string.append((char)(Math.abs(strings.get(key, 0))));      
      key = strings.get(key, 1);
    }   
    string.append((char)key);
    string.reverse();
  }

  public static LongStream wordsWithPrefix(CharSequence prefix)
  {
    long key = useWord(prefix, false);
    if (key==DB.NULL) return LongStream.empty();
    return StreamSupport.longStream(instance.new WordSpliterator(key), false);
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
          final long existing = Lexicon.strings.get(key, i+2);
          if (existing==0 || existing == DB.NULL) break;
          stack.push(Lexicon.strings.get(key, i+3));
          i+=2;
        }

        if (Lexicon.strings.get(key, 0)>0)
        {
          action.accept(key);
          return true;
        }
      }
    }
  } 
}
