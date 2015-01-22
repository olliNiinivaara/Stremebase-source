/*
 * You need:
 * JDK 8 or later: http://www.oracle.com/technetwork/java/javase/downloads/index.html 
 * stremebase.jar: https://github.com/olliNiinivaara/StremeBase/blob/master/stremebase.jar?raw=true  
 */

package com.stremebase.tutorials.basic;


import java.util.Scanner;

import com.stremebase.map.OneMap;
import com.stremebase.base.DB;


public class Chapter_1_Introduction
{
  private static final Scanner in = new Scanner(System.in);
  
  private static boolean persisted = false; 
  public static OneMap entity_primarykey;
  
  
  public static void main(String[] args)
  {
    welcome();
    lesson1_startingTheDB();
    lesson2_creatingAMap();
    lesson3_puttingValues();
    lesson4_gettingValues();
    lesson5_onperformance();
  }
  
  public static void welcome()
  {
    p("Stremebase v.0.1 Tutorial - Chapter 1 - introduction - welcome");
    p("");
    p("Stremebase is pronounced as Streambase. The name reflects it's stream-oriented design and it's xstreme simplicity.");
    p("");
    p("You create one DB object to represent the whole database and as many maps as there are individual attributes.");
    p("A map associates keys with values (it's a key value store).");
    p("You can think that a map represents one column (stremebase is a column-oriented database).");
    p("");
    p("Stremebase is simple: it does not know about entities (\"tables\")");
    p("");
    p("OK? ALWAYS PRESS ENTER WHEN YOU ARE READY TO CONTINUE THE TUTORIAL");
    in.nextLine();    
  }
  
  public static void lesson1_startingTheDB()
  {
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
    p("2: CREATING A MAP");
    p("");
    p("Stremebase does not store your information model - it is a schemaless database.");
    p("This makes it easy to create new maps on the fly as needed.");
    p("But this also means that you must deliver the model info in constructor.");
    p("And it would be disastrous to deliver wrong model for a map that is already persisted to disk!");
    p("Therefore be cautious that your map names are unique.");
    p("One good rule is to prefix the map name with the entity name that the attribute belongs to.");    
    p("");    
    p("To create a map called (say) entity_primarykey, we now call: "); 
    p("entity_primarykey = new OneMap(\"entity_primarykey\");");
    p("");
    
    entity_primarykey = new OneMap("entity_primarykey");
    
    p("If you wondered, OneMap is a type of map that allows just one value per key.");
    p("You'll learn about more complex maps in later tutorials...");
    
    in.nextLine();
  }
  
  public static void lesson3_puttingValues()
  {
    long value = 1;
    
    p("3: PUTTING VALUES");
    p("");
    
    p("Let's start by checking, if entity_primarykey contains an entry for value %d: ", value);
    p("if (entity_primarykey.containsValue(%d))...;", value);
    
    if (entity_primarykey.containsValue(value))
    {
      p("An entry already exists, we shall not create a duplicate (end of lesson).");
      in.nextLine();
      return;
    }
    
    p("No entry yet.");
    p("");
    p("To generate a new key, we first get the largest key in the map:");
    p("long key = entity_primarykey.getLargestKey();");

    long key = entity_primarykey.getLargestKey();

    p("");
    p("Then we add 1 to it:");
    p("key++;");

    key++;

    p("");
    p("And now we can associate our new key %d with the new value %d:", key, value);
    p("entity_primarykey.put(key, value)");

    entity_primarykey.put(key, value);

    p("");
    p("Internally Stremebase may cache data and use other tricks to speed up writing.");
    p("To flush the caches and restore consistency, you must call commit() between writes and reads.");
    p("You can commit individual maps like this: map.commit();");
    p("Or commit all maps in one shot by calling: DB.db.commit();");
    
    DB.db.commit();
    
    p(""); 
    p("If Stremebase is in persistent mode, commit() also guarantees that changes are flushed to disk.");
    
    p("");
    
    p("To make the next lesson more interesting, let's ensure that there are some more entries: ");
    p("for (long l = 0; l<100; l++) entity_primarykey.put(++key, l %% 20);");
    for (long l = 0; l<100; l++) entity_primarykey.put(++key, l % 20);
    entity_primarykey.commit();
    p("Committed!");  
    
    in.nextLine();      
  }
  
  public static void lesson4_gettingValues()
  {
    p("4: GETTING VALUES");
    p("");
    p("Let's start by getting the size of our map: entity_primarykey.getSize();");    
    p("There are currently %d entries", entity_primarykey.getSize());
    p("");
    
    if (entity_primarykey.getSize()==0)
    {
      p("Because the map appears to be empty, lesson 4 ends here.");
      in.nextLine();
      return;
    }
    
    p("We can stream all keys in ascending order with: entity_primarykey.keys()");
    entity_primarykey.keys().forEach(key->System.out.print(key+" "));
    p("");p("");
    
    p("We get the value associated with a key with: entity_primarykey.get(key);");
    p("Let's get them all: ");
    entity_primarykey.keys().forEach(key->System.out.print(key+"->"+entity_primarykey.get(key)+" "));
    p("");p("");
    
    p("If you try to get a value for a nonexistent key, you'll get value DB.NULL, which is %d", DB.NULL);
    p("Let's try: entity_primarykey.get(56745635654l) = %d", entity_primarykey.get(56745635654l));
    p("");
    
    p("The most important operation offered by Stremebase is query.");
    p("Queries select keys where a value exists between a defined range.");
    p("General syntax is: map.query(lowestValue, highestValue)");
    p("Examples: ");
    
    p("Select keys where value < 3 : entity_primarykey.query(Long.MIN_VALUE, 2)");
    entity_primarykey.query(Long.MIN_VALUE, 2).forEach(key->p(key+""));
    p("");
    
    p("Select keys where 12 <= value <= 15 : entity_primarykey.query(12, 15)");
    entity_primarykey.query(12, 15).forEach(key->p(key+""));
    p("");
    
    p("Select keys where value = 1 : entity_primarykey.query(1, 1)");
    entity_primarykey.query(1, 1).forEach(key->p(key+""));
    p("");
    p("If there's a big map under heavy querying, you can speed up things with an index.");
    p("You'll learn about indexing in later tutorials...");
    in.nextLine();
  }
  
  public static void lesson5_onperformance()
  {
    
  }
  
  private static void p(String format, Object... args)
  {
    System.out.printf(format+"%n", args);
  }
}
