package com.stremebase.base.util;

import java.util.stream.LongStream;

import com.stremebase.base.DB;
import com.stremebase.base.To;
import com.stremebase.map.SetMap;

public class TextIndex
{  
  protected final SetMap setMap;

  public TextIndex(String name, boolean persisted)
  {
    setMap = new SetMap(name, SetMap.SET, persisted);
  }

  public void commit()
  {
    setMap.commit();
  }

  public void clear()
  {
    setMap.clear();
  }

  public void add(CharSequence word, long document)
  {
    setMap.put(To.l(word), document);
  }

  public LongStream query(CharSequence... words)
  {
    LongStream[] streams = new LongStream[words.length];
    for (int i=0; i<words.length; i++)
    {
      final int l = words[i].length();
      if (l<2) continue;
      if (words[i].charAt(l-1) == DB.db.WILDENDING)
      {
        words[i] = words[i].subSequence(0, l-1);
        streams[i] = Streams.union(Lexicon.wordsWithPrefix(words[i]).mapToObj(word -> setMap.values(word)).toArray(LongStream[]::new));
      }
      else streams[i] = setMap.values(To.l(words[i]));
    }
    return Streams.intersection(streams).distinct();
  }
}
