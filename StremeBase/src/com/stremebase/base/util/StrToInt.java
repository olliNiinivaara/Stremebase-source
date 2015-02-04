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


import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

import com.stremebase.base.DB;
import com.stremebase.map.ListMap;


public class StrToInt
{	
	protected static final int width = 16*3+2;
		
	protected static ListMap strings;
	protected static final StringBuffer bufs = new StringBuffer();
	
	public static void initialize(boolean persist)
	{
	  if (strings == null) strings = new ListMap("Stremebase_string_index", width, DB.NOINDEX, DB.isPersisted());
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
		
	public static long useWord(CharSequence word, boolean put)
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
	
	public static void getWord(long key, StringBuilder string)
  {
	  string.setLength(0);
    if (key > Character.MAX_VALUE && strings.get(key, 0)<0) return;
    
    while (key > Character.MAX_VALUE)
    {     
      string.append((char)(Math.abs(strings.get(key, 0))));      
      key = strings.get(key, 1);
    }   
    string.append((char)key);
    string.reverse();
  }
	
	/*public static StringBuffer getWord(long key)
  {
		buf.setLength(0);
		if (key > Character.MAX_VALUE && strings.get(key, 0)<0) return buf;
		
  	while (key > Character.MAX_VALUE)
  	{  		
  		buf.append((char)(Math.abs(strings.get(key, 0))));
  	  
  	  key = strings.get(key, 1);
  	} 	
  	buf.append((char)key);
  	return buf.reverse();
  }
	
	public static String getText(long[] list)
  {
		if (list.length == 0) return null;
		bufs.setLength(0);
		for (long i: list) bufs.append(getWord(i).append(DB.db.TEXTBINDER));
		return bufs.substring(0, bufs.length()-1);
  }*/
	
	public static LongStream completeWords(CharSequence start)
	{
		long key = useWord(start, false);
		if (key==DB.NULL) return LongStream.empty();
		return StreamSupport.longStream(new WordSpliterator(key), false);
	}
	
	/*protected static void getCompleteWords(long key)
	{
		int i = 0;
		while (true)
		{
			final long existing = strings.get(key, i+2);
			if (existing==0 || existing == DB.NULL)
			{
				if (i==0) DB.out(getWord(key));
				return;
			}
			else
			{
				if (strings.get(key, 0)>0) DB.out(getWord(key));
				getCompleteWords(strings.get(key, i+3));
			}
			i+=2;
			if (i>=strings.getSize(key)-1) return;
		}
	}*/
}
