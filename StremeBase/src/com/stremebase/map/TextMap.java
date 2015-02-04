package com.stremebase.map;

import java.util.stream.LongStream;

import com.stremebase.base.util.StrToInt;

public class TextMap
{
  public static LongStream wordsWithPrefix(CharSequence prefix)
  {
    return StrToInt.completeWords(prefix);
  }
}
