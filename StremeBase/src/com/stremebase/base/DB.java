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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.stremebase.file.FileManager;
import com.stremebase.map.ArrayMap;
import com.stremebase.map.SetMap;

/**
 *The database
 *
 * @author olli
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
   * Whether database is persisted to disk or in in-memory mode.
   */
  public final boolean PERSISTED;

  /**
   * The metadata manager
   */
  public final Catalog catalog;

  /**
   * The string storage
   */
  public final Lexicon lexicon;

  /**
   * The value converter
   */
  public final To to;

  protected final FileManager fileManager;

  protected final Collection<AutoCloseable> closeables = new ArrayList<>(10);

  //----------------------------------------------------------------

  /**
   * A constant indicating that the value is missing.
   * You can write DB.NULL but you cannot query it.
   */
  public static final long NULL = Long.MIN_VALUE;

  /**
   * A constant indicating end of stream. Reserved value for future use (with TCP sockets).
   */
  public static final long EOF = Long.MIN_VALUE+1;

  /**
   * A constant indicating end of "line". Reserved value for future use (with TCP sockets).
   */
  public static final long EOL = Long.MIN_VALUE+2;

  /**
   * A constant indicating end of record. Reserved value for future use (with TCP sockets).
   */
  public static final long EOR = Long.MIN_VALUE+3;

  /**
   * The minimum allowable value.
   */
  public static final long MIN_VALUE = Long.MIN_VALUE+4;

  /**
   * The maximum allowable value, same as Long.MAX_VALUE
   */
  public static final long MAX_VALUE = Long.MAX_VALUE;


  /**
   * Relation type: No relation
   */
  public static final byte NO_INDEX = 0;

  /**
   * Relation type: Every attribute is associated with at most one value and vice versa
   */
  public static final byte ONE_TO_ONE = 1;

  /**
   * Relation type: Every attribute is associated with at most one value.
   */
  public static final byte MANY_TO_ONE = 2;

  /**
   * Relation type: Every value is associated with at most one attribute.
   */
  public static final byte ONE_TO_MANY = 3;

  /**
   * Relation type: Every attribute is associated with any value but at most once.
   */
  public static final byte MANY_TO_MANY = 4;


  /**
   * Constructor, override this for custom configurations
   * @param directory where the database is persisted on disk, null=in memory, "user.dir"=user.dir/db/
   */
  public DB(String directory)
  {
    PERSISTED = directory!=null;
    if (directory==null) directory="in-memory://";
    else if (directory.toLowerCase().equals("user.dir"))
    {
      directory = System.getProperty("user.dir")+File.separatorChar+"db"+File.separatorChar;
      System.out.println("Stremebase directory: "+directory);
    }
    DIRECTORY = directory;
    catalog = new Catalog(this);
    closeables.add(catalog);
    fileManager = new FileManager(catalog);
    closeables.add(fileManager);
    lexicon = new Lexicon(this);
    to = new To(this);

    if (PERSISTED) Runtime.getRuntime().addShutdownHook(
        new Thread()
        {
          @Override
          public void run()
          {
            flush();
            close();
          }
        });
  }

  /**
   * Constructor for in-memory database
   */
  public DB()
  {
    this(null);
  }

  /**
   * Check
   * @return true, if data is persisted to disk.
   */

  /**
   * Full map definition
   * @param mapName name of the map
   * @param mapClass Class of the map
   * @param properties Custom properties
   * @param overwrite whether custom properties should overwrite existing values
   */
  public void defineMap(String mapName, Class<? extends StremeMap> mapClass, Map<String, Object> properties, boolean overwrite)
  {
    catalog.defineMap(mapName, mapClass, properties, overwrite);
  }

  /**
   * Map definition without custom properties
   */
  public void defineMap(String mapName, Class<? extends StremeMap> mapClass)
  {
    defineMap(mapName, mapClass, null, false);
  }

  /**
   * Array map definition
   * @param mapName name
   * @param length of the array
   */
  public void defineArrayMap(String mapName, int length)
  {
    defineMap(mapName, ArrayMap.class, props().add(Catalog.NODESIZE, length+1).build(), false);
  }

  /**
   * Multiset map definition
   * @param mapName name
   */
  public void defineMultiSetMap(String mapName)
  {
    defineMap(mapName, SetMap.class, props().add(Catalog.SETTYPE, SetMap.MULTISET).build(), false);
  }

  /**
   * Multiset map definition with custom properties
   * @param mapName name
   * @param properties properties
   */
  public void defineMultiSetMap(String mapName, Map<String, Object> properties)
  {
    defineMap(mapName, SetMap.class, props(properties).add(Catalog.SETTYPE, SetMap.MULTISET).build(), false);
  }

  /**
   * Attributedset map definition
   * @param mapName name
   */
  public void defineAttributedSetMap(String mapName)
  {
    defineMap(mapName, SetMap.class, props().add(Catalog.SETTYPE, SetMap.ATTRIBUTEDSET).build(), false);
  }

  /**
   * Index definition
   * @param mapName name of the map
   * @param indexType Relation type of the index (DB.ONE_TO_ONE etc.)
   */
  public void defineIndex(String mapName, byte indexType)
  {
    catalog.getMap(mapName).addIndex(this, indexType);
  }

  /**
   * Index definition for array map cell
   * @param arrayMapName name
   * @param indexType Relation type of the index
   * @param cellIndex index of the cell to index
   */
  public void defineCellIndex(String arrayMapName, byte indexType, int cellIndex)
  {
    ((ArrayMap)catalog.getMap(arrayMapName)).addIndextoCell(this, indexType, cellIndex);
  }

  /**
   * Get a map
   * @param mapName name
   * @return map
   */
  public <T extends StremeMap> T getMap(String mapName)
  {
    T result = catalog.getMap(mapName);
    if (result==null) throw new RuntimeException("No map with name "+mapName+" is defined");
    return result;
  }

  /**
   * Forces flushing all data to disk. A shutdown hook is set that will automatically call this.
   */
  public void flush()
  {
    fileManager.flushAll();
  }

  /**
   * Will be called from shutdown hook
   * @param closeable Something that needs closing
   */
  public void addCloseable(AutoCloseable closeable)
  {
    closeables.add(closeable);
  }

  /**
   * Closes autocloseables. A shutdown hook is set that will automatically call this.
   */
  public void close()
  {
    for (AutoCloseable closeable: closeables) try {closeable.close();} catch (Exception e){e.printStackTrace();}
  }

  /**
   * Deletes all data, aka Drop Database.
   */
  public void clear()
  {
    fileManager.clearAll();
    if (PERSISTED) fileManager.deleteDir(new File(DIRECTORY));
  }

  /**
   * Whether the database is created
   * @return true if database exists
   */
  public boolean existsOnDisk()
  {
    return new File(DIRECTORY).exists();
  }

  /**
   * Helper method to define properties using method chaining
   * @return new PropertiesBuilder
   */
  public PropertiesBuilder props()
  {
    return new PropertiesBuilder();
  }

  /**
   * Helper method to define properties using method chaining
   * @param properties
   * @return new PropertiesBuilder with the properties
   */
  public PropertiesBuilder props(Map<String, Object> properties)
  {
    return new PropertiesBuilder(properties);
  }

  /**
   * Helper class to define properties using method chaining
   *
   * @author olli
   */
  public static class PropertiesBuilder
  {
    Map<String, Object> props;

    public PropertiesBuilder()
    {
      props = new HashMap<>();
    }

    public PropertiesBuilder(Map<String, Object> properties)
    {
      props = properties;
    }

    public PropertiesBuilder add(String property, Object value)
    {
      props.put(property, value);
      return this;
    }

    public Map<String, Object> build()
    {
      return props;
    }
  }
}
