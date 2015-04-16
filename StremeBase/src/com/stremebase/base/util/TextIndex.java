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

import com.stremebase.base.DB;
import com.stremebase.base.To;
import com.stremebase.map.SetMap;

/**
 * A simple utility class for indexing and querying documents (=texts=arrays of words).
 * Does not calculate relevance 
 */
public class TextIndex
{
  /**
   * The wildcard character that represents zero or more arbitrary characters at the end of word. (% in SQL).
   * <p>
   * Default: * (asterisk)
   */
  public char WILDENDING = '*';

  protected final SetMap setMap;

  /**
   * Constructor
   * @param name name
   * @param persisted persisted
   */
  public TextIndex(String name, boolean persisted)
  {
    setMap = new SetMap(name, DB.db.INITIALCAPACITY, SetMap.SET, persisted);
  }

  /**
   * Commits to disk
   */
  public void commit()
  {
    Lexicon.commit();
    setMap.commit();
  }

  /**
   * Deletes all data
   */
  public void clear()
  {
    setMap.clear();
  }

  /**
   * Associate a a word with a document
   * @param word the word
   * @param document the key for the document
   */
  public void add(CharSequence word, long document)
  {
    setMap.put(To.l(word), document);
  }

  /**
   * Returns document keys that match the search query
   * @param words the search words (AND-query), possibly ending with a wildcard
   * @return the matching documents
   */
  public LongStream query(CharSequence... words)
  {
    LongStream[] streams = new LongStream[words.length];
    for (int i=0; i<words.length; i++)
    {
      final int l = words[i].length();
      if (l<2) continue;
      if (words[i].charAt(l-1) == WILDENDING)
      {
        words[i] = words[i].subSequence(0, l-1);
        streams[i] = Streams.union(Lexicon.wordsWithPrefix(words[i]).mapToObj(word -> setMap.values(word)).toArray(LongStream[]::new));
      }
      else streams[i] = setMap.values(To.l(words[i]));
    }
    return Streams.intersection(streams).distinct();
  }
}
