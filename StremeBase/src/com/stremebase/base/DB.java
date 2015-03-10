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


import java.io.File;
import java.time.ZoneId;

import com.stremebase.base.util.Lexicon;
import com.stremebase.file.FileManager;

/**
 *DB is the class for controlling various configuration parameters.
 *If Stremebase is started by calling {@link #startDB(boolean persist) startDB(boolean persist)}, default values are used.
 *You can overrride default values with an inherited <code>DB</code> where you set the values in a custom constructor
 *and then calling {@link #startDB(DB dataBase, boolean persist) startDB(DB dataBase, boolean persist)} with it.
 *<p>
 *At runtime, the singleton instance can be accessed from {@link #db db}. Never modify it.
 */
public class DB
{	
  /**
   * Defines the folder where data is saved (if not in in-memory mode).
   * <p>
   * Default: new File(System.getProperty("user.dir")+File.separatorChar+"db"+File.separatorChar)   
   */
  public final String DIRECTORY;

  /**
   * Initial value size (= length = element count) for multi-valued properties.
   * <p>
   * Default: 100
   */
  public final int INITIALCAPACITY;

  /**
   * Initial set size (= length = element count) for indices' values.
   * <p>
   * Default: 10
   */
  //public final int INITIALINDEXVALUESIZE;

  /**
   * How many keys are included in one key file.
   * Optimal value depends at least on density of your keys, your data access patterns, size of your RAM and speed of your HD.
   * Basically, if keys run sequentially (as they should), the bigger the better. 
   * <p>
   * Default: 1000000  
   */	
  public final long KEYSTOAKEYFILE;

  /**
   * The start size for files that store data values.
   * Every time new file is needed, the size id doubled, until {@link #MAXVALUEFILESIZE MAXVALUEFILESIZE} is reached.
   * Optimal value depends at least on size and quantity of your data values, size of your RAM and speed of your HD.
   * Basically, if there's plenty of RAM, the bigger the better. 
   * <p>
   * Default: 10000000  
   */
  public final long INITIALVALUEFILESIZE;

  /**
   * The maximum size for files that store data values - unless a single value needs more space.
   * Optimal size depends at least on the quantity of different properties, your data access patterns, and size of your RAM.
   * If any file is bigger than your free off-heap RAM: disaster.  
   * <p>
   * Default: 64 * INITIALVALUEFILESIZE
   */
  public final long MAXVALUEFILESIZE;

  /**
   * The maximum number of deleted value slots that is remembered.
   * The smaller the value, the less on-heap memory is lost. If the value is too small, some deleted slots are not
   * recorded, which will leave gaps in files, which eats your HD space.  
   * <p>
   * Default: 100000
   */
  public final int MAXCACHEDFREESLOTS;

  /**
   * The maximum number of modified set values that are cached on-heap before written to off-heap.
   * Writing to off-heap requires ordering the values, which is slow. Therefore modifications are cached
   * until there's a read operation, <code>MAXCACHEDSETSIZE</code> is hit, or java heap size becomes scarce.
   * <p>
   * Default: 10000
   */
  public final int MAXCACHEDSETSIZE;

  /**
   * The maximum number of entries in each modified set value that are cached on-heap before written to off-heap.
   * <p>
   * Default: 1000
   */
  public final int MAXCACHEDSETVALUEENTRIES;


  public final int MINIMUMRELATIONSHIPSIZE;

  /**
   * The default regular expression for splitting texts to individual words.
   * <p>
   * Default: " " (space)
   */
  //public final String DEFAULTTEXTSPLITTER;

  /**
   * The character to put between words of texts that were split.
   * <p>
   * Default: ' ' (space)
   */
  public final char TEXTBINDER;

  /**
   * The wildcard character that represents zero or more arbitrary characters at the end of word. (% in SQL).
   * <p>
   * Default: * (asterisk)
   * see stremebase.property.api.Text#getKeysContainingWords(CharSequence... words)
   */
  //public final char WILDENDING;

  public final ZoneId ZONE;

  //----------------------------------------------------------------

  /**
   * This map constructor parameter tells that the map is not to be indexed.
   */
  public static final int NOINDEX = 0;

  /**
   * This map constructor parameter tells that the map will be indexed and same value will not appear multiple times for any key.
   */
  public static final int SIMPLEINDEX = 1;

  /**
   * This map constructor parameter tells that the map will be indexed and appearances of same values per key are counted.
   */
  public static final int MULTIINDEX = 2;

  /**
   * A constant indicating that the value is missing.
   */
  public static final long NULL = Long.MIN_VALUE;

  /**
   * DB.NULL value as String.
   *
	public static final String NULLLONG = Long.toString(NULL);*/

  private static boolean persisted = true;

  /**
   * Check whether database was started in persisted or in in-memory mode. 
   * @return true, if data is persisted to disk.
   */
  public static boolean isPersisted() {return persisted;}

  /**
   * The singleton FileManager instance.
   * For internal use.
   */
  public static final FileManager fileManager = new FileManager();

  /**
   * Access to the singleton DB instance at run-time.
   * For internal use.
   */
  public static DB db;


  /**
   * Constructor where all final parameters are set.
   */
  public DB()
  {
    DIRECTORY = System.getProperty("user.dir")+File.separatorChar+"db"+File.separatorChar;
    INITIALCAPACITY = 100;
    //INITIALINDEXVALUESIZE = 10;
    KEYSTOAKEYFILE = 1000000;

    INITIALVALUEFILESIZE = 10000000;	
    MAXVALUEFILESIZE = 64 * INITIALVALUEFILESIZE;

    MAXCACHEDFREESLOTS = 100000;		
    MAXCACHEDSETSIZE = 10000;
    MAXCACHEDSETVALUEENTRIES = 1000;

    MINIMUMRELATIONSHIPSIZE = 1000;

    //DEFAULTTEXTSPLITTER = " ";
    TEXTBINDER = ' ';
    //WILDENDING = '*';

    ZONE = ZoneId.systemDefault();
  }

  /**
   * Starts Stremebase with default parameter values.
   * @param persist Set to true, if you want a true persisted database. Set to false, if you want an in-memory database.
   */
  public static void startDB(boolean persist)
  {
    if (db!=null) return;
    startDB(new DB(), persist);	
  }

  /**
   * Starts Stremebase with custom settings (for advanced users).
   * @param dataBase DB with parameter values that are optimized for your application and hardware.
   * @param persist Set to true, if you want a true persisted database. Set to false, if you want an in-memory database.
   */
  public static void startDB(DB dataBase, boolean persist)
  {
    db = dataBase;
    persisted = persist;
    Lexicon.initialize(persist);

    if (persist) Runtime.getRuntime().addShutdownHook(
        new Thread()
        {
          public void run()
          {
            db.commit();
            db.close();
          }
        });
  }

  /**
   * Forces flushing of all memory-mapped files to disk. A shutdown hook is set that will
   * automatically call this, so you don't need to.
   */
  public void commit()
  {
    fileManager.commitAll();
  }

  /**
   * Stores information about free spaces in files to disk. A shutdown hook is set that will
   * automatically call this, so you don't need to.
   */
  public void close()
  {
    fileManager.closeAll();
  }

  /**
   * Deletes all data, aka Drop Database.
   */
  public void clear()
  {
    fileManager.clearAll();
    if (persisted) FileManager.deleteDir(new File(DIRECTORY));
  }
}
