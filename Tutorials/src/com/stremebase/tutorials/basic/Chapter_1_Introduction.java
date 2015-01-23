/*
 * You need:
 * JDK 8 or later: http://www.oracle.com/technetwork/java/javase/downloads/index.html 
 * stremebase.jar: https://github.com/olliNiinivaara/StremeBase/blob/master/stremebase.jar?raw=true  
 */

package com.stremebase.tutorials.basic;


import java.util.HashMap;
import java.util.Scanner;

import com.stremebase.map.OneMap;
import com.stremebase.base.DB;


public class Chapter_1_Introduction
{
  
  
  private static boolean persisted = false;  //try true also!
  private static long testsize = 10000000; 
  public static OneMap entity_attribute;
  
  private static final Scanner in = new Scanner(System.in);
  
  
  public static void main(String[] args)
  {
    welcome();
    lesson1_startingTheDB();
    lesson2_creatingAMap();
    lesson3_puttingValues();
    lesson4_gettingValues();
    lesson5_onperformance();
    recap();
    l();
    p("Bye!");
  }
  
  public static void welcome()
  {
    l();
    p("Stremebase v.0.1 Tutorial - Chapter 1: Introduction");
    p("");
    p("Stremebase is pronounced as Streambase. The name reflects it's stream-oriented design and it's xstreme simplicity.");
    p("");
    p("Stremebase is (almost) all about maps.");
    p("A map associates keys with values (it's a key value store).");
    p("A key is a unique identifier for entity instances (objects).");
    p("In practice you'll always assign keys by just choosing the next unused number available.");
    p("A value is an attribute for a class of entities. You'll learn about supported data types later.");
    p("You can think that a map represents one column - stremebase is a column-oriented database.");
    p("In graph db parlance, a map represents a property.");
    p("");
    p("Stremebase is simple: it does not handle entities (\"tables\" or \"vertices\")");
    p("(However it can handle relationships - Stremebase is a graph database - but let's skip that for now...)");
    p("");
    p("OK? ALWAYS PRESS ENTER WHEN YOU ARE READY TO CONTINUE THE TUTORIAL");
    in.nextLine();    
  }
  
  public static void lesson1_startingTheDB()
  {
    l();
    p("1: STARTING THE DB");
    p("");
    p("Stremebase can act as an in-memory store (like redis) or as a database that is persisted to disk (like SQLite).");
    p("You can develop your information model in in-memory mode and switch to persisted mode when it's stable.");
    p("");
    p("Starting the DB is the first thing your program must do. Otherwise you would run into some very strange exceptions...");
    p("");
    p("To start Stremebase in in-memory mode, we call: DB.startDB(false);");
    p("To start Stremebase in persisted mode, you would call: DB.startDB(true);");
    p("");
    
    DB.startDB(persisted);
    p("We just called DB.startDB(%b);", persisted);
    
    in.nextLine();      
  }
  
  public static void lesson2_creatingAMap()
  {
    l();
    p("2: CREATING A MAP");
    p("");
    p("Stremebase does not store your information model - it is a schemaless database.");
    p("This makes it easy to create new maps on the fly as needed.");
    p("But this also means that you must deliver the model info in constructor.");
    p("And it would be disastrous to deliver wrong model for a map that is already persisted to disk!");
    p("Therefore be cautious that your map names are unique.");
    p("One practical convention is to name the map as a concatenation of an entity name and an attribute name.");    
    p("");    
    p("To create a map called (say) entity_attribute, we now call: "); 
    p("LongMap entity_attribute = new OneMap(\"entity_attribute\");");
    p("");
    
    entity_attribute = new OneMap("entity_attribute");
    
    p("If you wondered, OneMap is a type of map that allows just one value per key.");
    p("You'll learn about more complex maps in following chapters...");
    
    in.nextLine();
  }
  
  public static void lesson3_puttingValues()
  {
    long value = 1;
    
    l();
    p("3: PUTTING VALUES");
    p("");
    
    p("Let's start by checking, if entity_attribute contains an entry for value %d: ", value);
    p("if (entity_attribute.containsValue(%d))...;", value);
    
    long key = entity_attribute.getLargestKey();
    
    if (entity_attribute.containsValue(value)) p("An entry already exists, we shall not create a duplicate.%n");
    else
    {

      p("No entry yet.");
      p("");
      p("To generate a new key, we first get the largest key in the map:");
      p("long key = entity_attribute.getLargestKey();");



      p("");
      p("Then we add 1 to it:");
      p("key++;");

      key++;

      p("");
      p("And now we can associate our new key %d with the new value %d:", key, value);
      p("entity_attribute.put(key, value)");

      entity_attribute.put(key, value);

      p("");
      p("Internally Stremebase may cache data and use other tricks to speed up writing.");
      p("To flush the caches and restore consistency, you must call commit() between writes and reads.");
      p("You can commit individual maps like this: map.commit();");
      p("Or commit all maps in one shot by calling: DB.db.commit();");

      DB.db.commit();

      p(""); 
      p("If Stremebase is in persistent mode, commit() also guarantees that changes are flushed to disk.");

      p("");
    }

    p("To make the next lesson more interesting, let's ensure that there are some more entries: ");
    p("for (long l = 0; l<100; l++) entity_attribute.put(++key, l %% 20);");
    for (long l = 0; l<100; l++) entity_attribute.put(++key, l % 20);
    entity_attribute.commit();
    p("Committed!");
    
    if (DB.isPersisted())
    {
      p("");
      p("Since you are running the tutorial in persistent mode, it is important to know how");
      p("you can get the location of the database on disk:");
      p("DB.db.DIRECTORY: "+DB.db.DIRECTORY);
    }  
    
    in.nextLine();      
  }
  
  public static void lesson4_gettingValues()
  {
    l();
    p("4: GETTING VALUES");
    p("");
    p("Let's start by getting the size of our map: entity_attribute.getSize();");    
    p("There are currently %d entries", entity_attribute.getSize());
    p("");
    
    if (entity_attribute.getSize()==0)
    {
      p("Because the map appears to be empty, lesson 4 ends here.");
      in.nextLine();
      return;
    }
    
    p("We can stream all keys in ascending order with: entity_attribute.keys()");
    entity_attribute.keys().forEach(key->System.out.print(key+" "));
    p("");p("");
    
    p("We get the value associated with a key with: entity_attribute.get(key);");
    p("Let's get them all: ");
    entity_attribute.keys().forEach(key->System.out.print(key+"->"+entity_attribute.get(key)+" "));
    p("");p("");
    
    p("If you try to get a value for a nonexistent key, you'll get value DB.NULL, which is %d", DB.NULL);
    p("Let's try: entity_attribute.get(56745635654l) = %d", entity_attribute.get(56745635654l));
    p("");
    
    p("The most important operation offered by Stremebase is query.");
    p("Queries select keys where a value exists between a defined range.");
    p("General syntax is: map.query(lowestValue, highestValue)");
    p("Examples: ");
    
    p("Select keys where value < 3 : entity_attribute.query(Long.MIN_VALUE, 2)");
    entity_attribute.query(Long.MIN_VALUE, 2).forEach(key->p(key+""));
    p("");
    
    p("Select keys where 12 <= value <= 15 : entity_attribute.query(12, 15)");
    entity_attribute.query(12, 15).forEach(key->p(key+""));
    p("");
    
    p("Select keys where value = 1 : entity_attribute.query(1, 1)");
    entity_attribute.query(1, 1).forEach(key->p(key+""));
    p("");
    p("If there's a big map under heavy querying, you can speed up things with an index.");
    p("You'll learn about indexing later on...");
    in.nextLine();
  }
  
  public static void lesson5_onperformance()
  {
    l();
    p("5: ON PERFORMANCE");
    p("");
    p("I hope you are by now starting to appreciate the usability of the Stremebase API.");
    p("But maybe you wonder: Is Stremebase performant?");
    p("Well, comparing database performance is very hard.");
    p("But what the heck, let's compare StremeBase's OneMap to Java Collection Framework's HashMap!");
    p("");
    testHashMap();
    testOneMap();   
  }
  
  protected static void testHashMap()
  {
    final long foo = testsize+1;
    
    p("Putting %d entries to a HashMap", testsize);
    HashMap<Long, Long> map = new HashMap<>();
    long key;    
    System.gc();
    p("Ready...Set...Go!");
    long start = System.currentTimeMillis();
    for (key=0; key<testsize; key++) map.put(key, key);
    long end = System.currentTimeMillis();
    p("Finished!");
    p("Put time for a HashMap: %d milliseconds", end-start);    
    p("");
    p("Getting all %d entries from a HashMap", testsize);
    System.gc();
    p("Ready...Set...Go!");
    start = System.currentTimeMillis();
    map.keySet().forEach(k-> {if (map.get(k)%foo==foo) p("Never happens");});
    end = System.currentTimeMillis();
    p("Finished!");
    p("Get time for a HashMap: %d milliseconds", end-start);    
    p("");
  }
  
  protected static void testOneMap()
  {
    final long foo = testsize+1;
    
    p("Putting %d entries to a OneMap", testsize);
    OneMap map = new OneMap("oneMap");
    long key;    
    System.gc();
    p("Ready...Set...Go!");
    long start = System.currentTimeMillis();
    for (key=0; key<testsize; key++) map.put(key, key);
    long end = System.currentTimeMillis();
    p("Finished!");
    p("Put time for a OneMap: %d milliseconds", end-start);    
    p("");
    p("Getting all %d entries from a OneMap", testsize);
    System.gc();
    p("Ready...Set...Go!");
    start = System.currentTimeMillis();
    map.keyset().forEach(k-> {if (map.get(k)%foo==foo) p("Never happens");});
    map.commit();
    end = System.currentTimeMillis();
    p("Finished!");
    p("Get time for a OneMap: %d milliseconds", end-start);    
    p("");
    if (DB.isPersisted())
    {
      p("Last but not least, let's remove the test data from your hard disk: map.clear();");
      map.clear();
    }
  }
  
  public static void recap()
  {
    l();
    p("6: RECAP");
    p("This is the end of the introduction tutorial.");
    p("Thank you for your attention.");
    p("The title of chapter 2 is: Supported data types");
    p("See you there!");
    p("");
    p("Here's a complete method template for you to experiment with: ");
    p("");
        
    p("DB.startDB(false);");
    p("OneMap map = new OneMap(\"AVeryUniqueName\");");
    p("map.put(map.getLargestKey()+1, 1);");
    p("map.commit();");
    p("System.out.printf(\"size: %%d%%n\", map.getSize());");
    p("System.out.print(\"Keys: \");");
    p("map.keys().forEach(key->System.out.print(key+\" \"));");
    p("System.out.println();");
    p("System.out.print(\"Entries: \");");
    p("map.keys().forEach(key->System.out.print(key+\"->\"+map.get(key)+\" \"));");
    p("System.out.println();");
    p("System.out.print(\"Keys where value <= 100: \");");    
    p("map.query(Long.MIN_VALUE, 100).forEach(key->System.out.print(key+\" \"));");
    p("System.out.println();");
    p("map.clear();");
    p("");
    p("If you run it verbatim, it will output:");
    DB.startDB(false);  //Does nothing, if DB is already started
    OneMap map = new OneMap("AVeryUniqueName");
    map.put(map.getLargestKey()+1, 1);
    map.commit();
    System.out.printf("size: %d%n", map.getSize());
    System.out.print("Keys: ");
    map.keys().forEach(key->System.out.print(key+" "));
    System.out.println();
    System.out.print("Entries: ");
    map.keys().forEach(key->System.out.print(key+"->"+map.get(key)+" "));
    System.out.println();
    System.out.print("Keys where value <= 100: ");    
    map.query(Long.MIN_VALUE, 100).forEach(key->System.out.print(key+" "));
    System.out.println();
    map.clear();
  }
    
  
  private static void p(String format, Object... args)
  {
    System.out.printf(format+"%n", args);
  }
  
  private static void l()
  {
    System.out.println("\n---------------------------------------------------------------");
  }
}
