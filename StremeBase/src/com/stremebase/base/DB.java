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
  public final int INITIALINDEXVALUESIZE;

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
   * Default: 2 gigas
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
   * until there's a commit, read operation or <code>MAXCACHEDSETSIZE</code> is hit.
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

  /**
   * Time-zone when converting between Instants and LocalDateTimes
   * <p>
   * Default: ZoneId.systemDefault();
   */
  public final ZoneId ZONE;

  //----------------------------------------------------------------

  /**
   * A constant indicating that the value is missing.
   * Note that you cannot even query for DB.NULL values.
   */
  public static final long NULL = Long.MIN_VALUE;

  /**
   * Index type: No index
   */
  public static final byte NO_INDEX = 0;

  /**
   * Index type: Every key is associated with at most one value and vice versa
   */
  public static final byte ONE_TO_ONE = 1;

  /**
   * Index type: Every key is associated with at most one value.
   */
  public static final byte MANY_TO_ONE = 2;

  /**
   * Index type: Every value is associated with at most one key.
   */
  public static final byte ONE_TO_MANY = 3;

  /**
   * Index type: Every key is associated with any value but at most once.
   */
  public static final byte MANY_TO_MANY = 4;

  /**
   * Index type: A key may be associated even with the same value more than once.
   */
  public static final byte MANY_TO_MULTIMANY = 5;

  private static boolean persisted = true;

  /**
   * Check whether database was started in persisted or in in-memory mode.
   * @return true, if data is persisted to disk.
   */
  public static boolean isPersisted() {return persisted;}

  /**
   * The singleton FileManager instance.
   * For internal use only.
   */
  public static final FileManager fileManager = new FileManager();

  /**
   * Access to the singleton DB instance at run-time.
   * Mainly for internal use.
   */
  public static DB db;


  /**
   * Constructor where all final parameters are set.
   */
  public DB()
  {
    DIRECTORY = System.getProperty("user.dir")+File.separatorChar+"db"+File.separatorChar;
    INITIALCAPACITY = 100;
    INITIALINDEXVALUESIZE = 10;
    KEYSTOAKEYFILE = 1000000;

    INITIALVALUEFILESIZE = 10000000;
    MAXVALUEFILESIZE = Integer.MAX_VALUE / 2;

    MAXCACHEDFREESLOTS = 100000;
    MAXCACHEDSETSIZE = 10000;
    MAXCACHEDSETVALUEENTRIES = 1000;

    ZONE = ZoneId.systemDefault();
  }

  /**
   * Starts Stremebase with default parameter values.
   * @param persist Set to true, if you want a true persisted database. Set to false, if you want an in-memory database.
   */
  public static void startDB(boolean persist)
  {
    startDB(new DB(), persist);
  }

  /**
   * Starts Stremebase with custom settings.
   * @param dataBase DB with parameter values that are optimized for your application and hardware.
   * @param persist Set to true, if you want a true persisted database. Set to false, if you want an in-memory database.
   */
  public static void startDB(DB dataBase, boolean persist)
  {
    if (db!=null) return;

    db = dataBase;
    persisted = persist;
    Lexicon.initialize(persist);

    if (persist) Runtime.getRuntime().addShutdownHook(
        new Thread()
        {
          @Override
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

  public boolean existsOnDisk()
  {
    return new File(DIRECTORY).exists();
  }
}
