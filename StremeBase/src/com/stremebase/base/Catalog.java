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
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.stremebase.map.SetMap;

/**
 * Gets and sets performance-critical metadata for maps, such as buffer sizes.
 *
 * @author olli
 */
public class Catalog implements AutoCloseable
{
  public static final String INITIALCAPACITY = "INITIALCAPACITY";
  public static final String INITIALINDEXVALUESIZE = "INITIALINDEXVALUESIZE";
  public static final String KEYSTOAKEYFILE = "KEYSTOAKEYFILE";
  public static final String KEYSTOAARRAYKEYFILE = "KEYSTOAARRAYKEYFILE";
  public static final String KEYSTOAMEMORYANDINDEXKEYFILE = "KEYSTOAMEMORYANDINDEXKEYFILE";
  public static final String INITIALVALUEFILESIZE = "INITIALVALUEFILESIZE";
  public static final String INITIALMEMORYANDINDEXVALUEFILESIZE = "INITIALMEMORYANDINDEXVALUEFILESIZE";
  public static final String MAXVALUEFILESIZE = "MAXVALUEFILESIZE";
  public static final String MAXCACHEDFREESLOTS = "MAXCACHEDFREESLOTS";
  public static final String MAXCACHEDSETSIZE = "MAXCACHEDSETSIZE";
  public static final String MAXCACHEDSETVALUEENTRIES = "MAXCACHEDSETVALUEENTRIES";
  public static final String DIRECTORY = "DIRECTORY";
  public static final String NODESIZE = "NODESIZE";
  public static final String PERSISTED = "PERSISTED";
  public static final String MAPTYPE = "MAPTYPE";
  public static final String SETTYPE = "SETTYPE";
  public static final String INDEXTYPE = "INDEXTYPE";

  public final DB db;


  protected final Map<String, StremeMap> maps = new HashMap<>();
  protected final Map<StremeMap, String> keyDirectories = new HashMap<>();
  protected final Map<StremeMap, String> valueDirectories = new HashMap<>();
  protected final Map<StremeMap, String> freeSlotDirectories = new HashMap<>();

  private final Map<String, Object> systemProperties = new HashMap<>();
  private final Map<StremeMap, Map<String, Object>> mapProperties = new HashMap<>();

  protected Catalog(DB db)
  {
    this.db = db;
    Properties props = loadProperties(null);
    if (props!=null) readProperties(props, systemProperties);
    setDefaultSystemProperties();
  }

  /**
   * Defines a property for a map
   * @param property the property
   * @param map the map
   * @param value the value
   */
  public void setProperty(String property, StremeMap map, Object value)
  {
    mapProperties.get(map).put(property, value);
  }

  /**
   * Whether the map is already created
   * @param mapName the name of the map
   * @return true if map is created
   */
  public boolean mapExistsOnDisk(String mapName)
  {
    return new File(getDir(mapName)).exists();
  }

  /**
   * Every map must be defined once before they can be used. This is called from DB.defineMap -methods.
   * @param mapName the name of the map
   * @param mapClass the Class of the map
   * @param newProperties properties for the map
   * @param overwrite whether properties should overwrite existing property values (which is dangerous)
   */
  @SuppressWarnings("unchecked")
  public <T extends StremeMap> void defineMap(String mapName, Class<? extends StremeMap> mapClass, Map<String, Object> newProperties, boolean overwrite)
  {
    if (maps.containsKey(mapName)) throw new RuntimeException("Map "+mapName+" is already defined.");

    Properties props = loadProperties(mapName);
    Map<String, Object> properties = new HashMap<>();
    readProperties(props, properties);

    if (mapClass==null)
    {
      if (properties.isEmpty()) throw new RuntimeException("Map called '"+mapName+"' has never been defined.");
      mapClass = (Class<? extends StremeMap>) properties.get(MAPTYPE);
    }

    StremeMap map = null;

    try
    {
      map = mapClass.newInstance();
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }

    if (newProperties!=null && (properties.isEmpty() || overwrite)) properties.putAll(newProperties);

    properties.put(MAPTYPE, mapClass);

    String directory = (String)properties.get(DIRECTORY);

    if (directory==null) directory = getDir(mapName);
    else
    {
      directory = directory.substring(2);
      if (!directory.endsWith(""+File.separatorChar)) directory += File.separatorChar;
    }
    keyDirectories.put(map, directory+"K"+File.separatorChar);
    valueDirectories.put(map, directory+"V"+File.separatorChar);
    freeSlotDirectories.put(map, directory+"F"+File.separatorChar);

    mapProperties.put(map, properties);

    map.initialize(mapName, this);
    maps.put(mapName, map);
  }

  protected void loadMap(String mapName)
  {
    defineMap(mapName, null, null, false);
    StremeMap map = getMap(mapName);

    Byte indexType = (Byte) getProperty(INDEXTYPE, map);
    if (indexType!=null && indexType!=DB.NO_INDEX) map.addIndex(db, (byte) getProperty(INDEXTYPE, map));
  }

  public void registerIndex(StremeMap map, byte indexType)
  {
    putProperty(INDEXTYPE, map, indexType);
  }

  /**
   * Gets a map. Available from DB.
   * @param mapName the map
   * @return the map
   */
  @SuppressWarnings("unchecked")
  public <T extends StremeMap> T getMap(String mapName)
  {
    T map = (T)maps.get(mapName);
    if (map == null)
    {
      loadMap(mapName);
      map = (T)maps.get(mapName);
      if (map==null) throw new RuntimeException("Map with name "+mapName+" has not been defined.");
    }
    return map;
  }

  /**
   * Gets property value for a map
   * @param property the property name
   * @param map the map
   * @return the value
   */
  public Object getProperty(String property, StremeMap map)
  {
    return mapProperties.get(map).getOrDefault(property, systemProperties.get(property));
  }

  /**
   * Puts a property
   * @param property the property name
   * @param map the map
   * @param value the value
   */
  public void putProperty(String property, StremeMap map, Object value)
  {
    mapProperties.get(map).put(property, value);
  }

  /**
   * Used internally by FileManager
   */
  public String getDirectory(StremeMap map, char type)
  {
    if (type=='\0') return getDir(map.mapName);
    else if (type=='K') return keyDirectories.get(map);
    else if (type=='V') return valueDirectories.get(map);
    else if (type=='F') return freeSlotDirectories.get(map);
    return null;
  }

  protected Properties loadProperties(String mapName)
  {
    File file;
    if (mapName == null) file = new File(db.DIRECTORY+"systemProperties.txt");
    else file = new File(getDir(mapName)+mapName+"_properties.txt");

    Properties props = null;

    if (file.exists()) try
    {
      FileReader reader = new FileReader(file);
      props = new Properties();
      props.load(reader);
      reader.close();
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
      props = null;
    }
    return props;
  }

  /**
   * Writes properties to file. Automatically called when DB is closing.
   */
  @Override
  public void close()
  {
    if (!db.PERSISTED) return;
    File file;

    try
    {
      file = new File(db.DIRECTORY);
      file.mkdir();
      file = new File(db.DIRECTORY+"systemProperties.txt");
      file.createNewFile();
      FileWriter writer = new FileWriter(file);
      Properties props = new Properties();
      writeProperties(systemProperties, props);
      props.store(writer, null);
      writer.close();
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
    }

    for (Entry<StremeMap, Map<String, Object>> e: mapProperties.entrySet())
    {
      File dir = new File(getDir(e.getKey().mapName));
      dir.mkdir();
      file = new File(getDir(e.getKey().mapName)+e.getKey().mapName+"_properties.txt");
      try
      {
        file.createNewFile();
        FileWriter writer = new FileWriter(file);
        Properties props = new Properties();
        writeProperties(e.getValue(), props);
        props.store(writer, null);
        writer.close();
      }
      catch (Exception ex)
      {
        ex.printStackTrace();
      }
    }
  }

  protected void readProperties(Properties from, Map<String, Object> to)
  {
    if (from == null) return;

    for (Object p: from.keySet())
    {
      String prop = (String)p;
      String value = (String) from.get(prop);
      if (value==null) continue;
      char type = value.charAt(0);
      if (type=='l') to.put(prop, Long.parseLong(value.substring(2)));
      else if (type=='i') to.put(prop, Integer.parseInt(value.substring(2)));
      else if (type=='y') to.put(prop, Byte.parseByte(value.substring(2)));
      else if (type=='s') to.put(prop, value.substring(2));
      else if (type=='b') to.put(prop, Boolean.parseBoolean(value.substring(2)));
      else if (type=='c') try
      {
        to.put(prop, Class.forName(value.substring(2)));
      }
      catch (ClassNotFoundException e)
      {
        e.printStackTrace();
        continue;
      }
      else throw new RuntimeException("Unrecognized property type: "+type);
    }
  }

  protected void writeProperties(Map<String, Object> from, Properties to)
  {
    for (String prop: from.keySet())
    {
      Object value = from.get(prop);
      if (value==null) continue;
      if (value instanceof Long) to.setProperty(prop, "l-"+value.toString());
      else if (value instanceof Integer) to.setProperty(prop, "i-"+value.toString());
      else if (value instanceof Byte) to.setProperty(prop, "y-"+value.toString());
      else if (value instanceof Boolean) to.setProperty(prop, "b-"+value.toString());
      else if (value instanceof String) to.setProperty(prop, "s-"+value.toString());
      else if (value instanceof Class) to.setProperty(prop, "c-"+((Class<?>)value).getName());
      else throw new RuntimeException("Unrecognized property type: "+value.getClass().getName());
    }
  }

  /**
   * Sets default properties for maps. Override this to trim StremeBase memory usage and performance.
   */
  public void setDefaultSystemProperties()
  {
    systemProperties.putIfAbsent(INITIALCAPACITY, 100);
    systemProperties.putIfAbsent(INITIALINDEXVALUESIZE, 10);
    systemProperties.putIfAbsent(KEYSTOAKEYFILE, 1000000l);
    systemProperties.putIfAbsent(KEYSTOAARRAYKEYFILE, 50000l);
    systemProperties.putIfAbsent(KEYSTOAMEMORYANDINDEXKEYFILE, 10000l);
    systemProperties.putIfAbsent(INITIALVALUEFILESIZE, 10000000);
    systemProperties.putIfAbsent(INITIALMEMORYANDINDEXVALUEFILESIZE, 100000);
    systemProperties.putIfAbsent(MAXVALUEFILESIZE, Integer.MAX_VALUE / 2l);
    systemProperties.putIfAbsent(MAXCACHEDFREESLOTS, 100000);
    systemProperties.putIfAbsent(MAXCACHEDSETSIZE, 10000);
    systemProperties.putIfAbsent(MAXCACHEDSETVALUEENTRIES, 1000);
    systemProperties.putIfAbsent(PERSISTED, true);
    systemProperties.putIfAbsent(SETTYPE, SetMap.SET);
  }

  protected String getDir(String mapName)
  {
    return db.DIRECTORY+mapName+File.separatorChar;
  }
}
