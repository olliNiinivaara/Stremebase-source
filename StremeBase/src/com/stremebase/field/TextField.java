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
package com.stremebase.field;

import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.stremebase.base.Catalog;
import com.stremebase.base.DB;
import com.stremebase.base.Lexicon;
import com.stremebase.base.To;
import com.stremebase.dal.Field;
import com.stremebase.dal.KeySpace;
import com.stremebase.dal.Table;
import com.stremebase.dal.Value;
import com.stremebase.dal.KeySpace.SharedKey;
import com.stremebase.map.SetMap;
import com.stremebase.map.StackListMap;
import com.stremebase.util.Streams;

/**
 * A field to store and retrieve longer texts.
 * @author olli
 *
 */
@SuppressWarnings("rawtypes")
public class TextField extends Field
{
  public final StackListMap stackListMap;

  /**
   * If true, the first letters of every word are uppercased
   */
  public boolean capitalize;

  /**
   * The regex to split text into strings (words/terms/tokens). Default value " ".
   */
  public String wordSplitter= " ";

  /**
   * Never let applications to store strings of unlimited size. Default max length 100.
   */
  public int maxWordLength = 100;

  protected final StringBuilder sb = new StringBuilder();
  protected TextIndex textIndex;


  /**
   * Defines the field
   * @param table the table
   * @param name the name
   * @param textIndex the indexer. null means no indexing. TextIndex uses KeySpace and therefore can index and retrieve texts from multiple tables at once.
   */
  public TextField(Table table, String name, TextIndex textIndex)
  {
    this(table, name, 10, textIndex);
  }

  /**
   * Defines the field and initialCapacity (if not defined) 
   * @param table the table
   * @param name the name
   * @param initialCapacity Initial count of words for which space is reserved. Default value set by {@link Catalog} is 100 words. 
   * @param textIndex the textIndex to use
   */
  @SuppressWarnings("unchecked")
  public TextField(Table table, String name, int initialCapacity, TextIndex textIndex)
  {
    super(table, name);
    table.tableDb.defineMap(table.name+"_"+name, StackListMap.class, table.tableDb.props().add(Catalog.INITIALCAPACITY, initialCapacity).build(), false);
    map = table.tableDb.getMap(table.name+"_"+name);
    stackListMap = (StackListMap)map;
    this.textIndex = textIndex;
  }

  /**
   * Sets the text, splitting it using wordSplitter
   * @param key the key
   * @param text the text
   */
  public void set(long key, String text)
  {
    set(key, text, false);
  }

  /**
   * Gets the text
   * @param key the key
   * @return the text
   */
  public String get(long key)
  {
    long[] longs = stackListMap.get(key);
    if (longs==null) return null;
    if (longs.length==1 && longs[0]==0) return "";
    sb.setLength(0);

    for (int i=0; i<longs.length-1; i++) sb.append(Value.to.string(longs[i], capitalize)).append(wordSplitter);
    sb.append(Value.to.string(longs[longs.length-1], capitalize));

    return sb.toString();
  }

  /**
   * Appends text to existing text
   * @param key the key
   * @param text text to append
   */
  public void append(long key, String text)
  {
    set(key, text, true);
  }

  /**
   * Searches for word occurrences, see also {@link TextIndex} 
   * @param key the key
   * @param words the words
   * @return true if contains all the words
   */
  public boolean containsWords(long key, String... words)
  {
    return stackListMap.containsValues(key, Value.asLongArrayIfExists(words));
  }

  protected void set(long key, String text, boolean append)
  {
    if (text==null)
    {
      if (!append)
      {
        if (textIndex!=null) stackListMap.values(key).forEach( word-> textIndex.remove(word, table, key));
        stackListMap.remove(key);
        super.setModified(true);
      }
      return;
    }

    if (!append && textIndex!=null) stackListMap.values(key).forEach( word-> textIndex.remove(word, table, key));

    if (text.isEmpty())
    {
      if (!append)
      {
        stackListMap.put(key, 0, 0);
        stackListMap.shrinkValueSize(key, 1);
        super.setModified(true);
      }
      return;
    }

    int index = 0;
    if (append) index = (int) stackListMap.getValueCount(key);

    String[] words = text.split(wordSplitter);
    if (maxWordLength>0 && maxWordLength<Integer.MAX_VALUE) for (int i=0; i<words.length; i++) if (words[i].length()>maxWordLength) words[i] = words[i].substring(0, maxWordLength);
    for (int i=0; i<words.length; i++)
    {
      long word = Value.to.data(words[i]);
      stackListMap.put(key, index+i, word);
      if (textIndex!=null) textIndex.add(word, table, key);
    }
    if (!append) stackListMap.shrinkValueSize(key, words.length);

    super.setModified(true);
  }

  @Override
  protected void setModified(boolean modified)
  {
    if (autoFlush && modified) if (textIndex!=null) textIndex.flush(); else table.tableDb.lexicon.commit();
    super.setModified(modified);
  }

  /**
   * TextField cannot be indexed (use {@link TextIndex}).
   */
  @Override
  @Deprecated
  public void addIndex(byte indexType)
  {
    throw new UnsupportedOperationException("Textfields are indexed with a TextIndex.");
  }

  /**
   * A simple utility class for indexing and querying documents (=texts=arrays of words).
   * <p>
   * Does not calculate relevance
   */
  public static class TextIndex
  {
    /**
     * The wildcard character that represents zero or more arbitrary characters at the end of word. (% in SQL).
     * <p>
     * Default: * (asterisk)
     */
    public char WILDENDING = '*';

    protected final SetMap setMap;
    protected final Lexicon lexicon;
    protected final To to;

    /**
     * Constructor
     * @param db db
     * @param name name
     */
    public TextIndex(DB db, String name)
    {
      db.defineMap(name, SetMap.class);
      setMap = db.getMap(name);
      lexicon = db.lexicon;
      this.to = db.to;
    }

    /**
     * Commits to disk
     */
    public void flush()
    {
      lexicon.commit();
      setMap.flush();
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
     * @param table the table
     * @param key the key
     */
    public void add(long word, Table table, long key)
    {
      setMap.put(word, KeySpace.getGlobalKey(table, key));
    }

    /**
     * Removes association
     * @param word the word
     * @param table the table
     * @param key the key
     */
    public void remove(long word, Table table, long key)
    {
      setMap.removeValue(word, KeySpace.getGlobalKey(table, key));
    }

    /**
     * Returns document keys that match the search query
     * @param words the search words (AND-query), possibly ending with a wildcard
     * @return the matching documents
     */
    public Stream<SharedKey> search(String... words)
    {
      LongStream[] streams = new LongStream[words.length];
      for (int i=0; i<words.length; i++)
      {
        final int l = words[i].length();
        if (l<2) continue;
        if (words[i].charAt(l-1) == WILDENDING)
        {
          words[i] = words[i].substring(0, l-1);
          streams[i] = Streams.union(lexicon.wordsWithPrefix(words[i]).mapToObj(word -> setMap.values(word)).toArray(LongStream[]::new));
        }
        else streams[i] = setMap.values(to.data(words[i]));
      }
      return Streams.intersection(streams).distinct().mapToObj(globalKey->new SharedKey(globalKey));
    }

    /**
     * Search results limited to given table
     * @param table the table
     * @param words the search terms
     * @return the result
     */
    public Stream<SharedKey> search(Table table, String... words)
    {
      return search(words).filter(spaceKey -> (spaceKey.table==table));
    }
  }
}
