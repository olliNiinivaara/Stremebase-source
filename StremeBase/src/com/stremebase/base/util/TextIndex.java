package com.stremebase.base.util;

import java.util.stream.LongStream;
import com.stremebase.base.To;
import com.stremebase.map.SetMap;

public class TextIndex
{  
  protected final boolean relevanced;
  protected final SetMap setMap;

  public TextIndex(String name, boolean relevanced, boolean persisted)
  {
    this.relevanced = relevanced;
    if (relevanced) setMap = new SetMap(name, SetMap.MULTISET, persisted);
    else  setMap = new SetMap(name, SetMap.SET, persisted);
  }

  public void commit()
  {
    setMap.commit();
  }

  public void add(CharSequence word, long document)
  {
    setMap.put(To.l(word), document);
  }

  public LongStream query(CharSequence... words)
  {
    LongStream[] streams = new LongStream[words.length];
    for (int i=0; i<words.length; i++) streams[i] = setMap.values(To.l(words[i]));
    return Streams.intersection(streams);
  }
}
