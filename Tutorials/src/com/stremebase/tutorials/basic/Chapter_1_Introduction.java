/*
 * You need:
 * JDK 8 or later: http://www.oracle.com/technetwork/java/javase/downloads/index.html 
 * stremebase.jar: https://github.com/olliNiinivaara/StremeBase/blob/master/stremebase.jar?raw=true  
 */

package com.stremebase.tutorials.basic;



import java.util.Scanner;

import com.stremebase.map.LongMap;

import com.stremebase.base.DB;


public class Chapter_1_Introduction
{
  private static final Scanner in = new Scanner(System.in);
  
  public static LongMap tutorialtable_primarykey;
  
  
  public static void main(String[] args)
  {
    welcome();
    lesson1_startingTheDB();
    lesson2_creatingAMap();
    lesson3_crud();
    lesson4_query();
    lesson5_onperformance();
  }
  
  public static void welcome()
  {
    p("Stremebase v.0.1 Tutorial - Chapter 1 - introduction - welcome");
    p("");
    p("Stremebase is pronounced as Streambase. The name reflects it's stream-oriented design and it's xstreme simplicity.");
    p("");
    p("Stremebase consists of two classes: DB represents the whole database and LongMaps represent individual datasets.");
    p("LongMap is a map from keys to values (a key value store).");
    p("In Stremebase keys are always positive primitive longs.");
    p("In LongMap, value is also a primitive long.");
    p("");
    p("You can think that one map represents one column (stremebase is a column-oriented database).");
    p("");
    p("ALWAYS PRESS ENTER WHEN YOU ARE READY TO CONTINUE THE TUTORIAL");
    in.nextLine();    
  }
  
  public static void lesson1_startingTheDB()
  {
    p("1: STARTING THE DB");
    p("");
    p("Stremebase can act as an in-memory store (like redis) or as a database that is persisted to disk (like SQLite).");
    p("It makes sense to develop your information model in in-memory mode and switch to persisted mode when it's stable.");
    p("");
    p("Starting the DB is the first thing you must do. Otherwise you would run into some very strange exceptions...");
    p("");
    p("To start Stremebase in in-memory mode, we call: DB.startDB(false);");
    p("To start Stremebase in persisted mode, you would call: DB.startDB(true);");
    p("");
    p("(In this tutorial, we are using the in-memory mode)");
    p("");
    
    DB.startDB(false); //set to true to persist to disk
    
    in.nextLine();      
  }
  
  public static void lesson2_creatingAMap()
  {
    p("2: CREATING A MAP");
    p("");
    p("Stremebase does not store your information model - it is a schemaless database.");
    p("This makes it easy to create new maps (columns) on the fly as needed.");
    p("But this also means that you must deliver the model info in constructor.");
    p("And it would be disastrous to deliver wrong model for a map that is already persisted to disk!");
    p("");    
    p("To create a LongMap called tutorialtable_primarykey, we now call: "); 
    p("tutorialtable_primarykey = new LongMap(\"tutorialtable_primarykey\");");
    p("");
    
    tutorialtable_primarykey = new LongMap("tutorialtable_primarykey");
    
    in.nextLine();
  }
  
  public static void lesson3_crud()
  {
    long value = 1;
    
    p("3: CRUD");
    p("");
    
    p("Let's start by checking, if tutorialtable_primarykey contains an entry for value %d: ", value);
    p("if (tutorialtable_primarykey.containsValue(1))...;");
    
    if (tutorialtable_primarykey.containsValue(1)) p("An entry for value already 1 exists, we shall not create a duplicate.");
    else
    {
      p("No entry for value 1 yet.");
      p("");
      p("To generate a new key, we first get the largest key in the map:");
      p("long key = tutorialtable_primarykey.getLargestKey();");
          
      long key = tutorialtable_primarykey.getLargestKey();
      
      p("");
      p("Then we add 1 to it:");
      p("key++;");
      
      key++;
      
      p("");
      p("And now we can associate our new key with the new value:");
      p("tutorialtable_primarykey.put(key, value");
    }      
  }
  
  public static void lesson4_query()
  {
    p("To Be Continued..."); 
  }
  
  public static void lesson5_onperformance()
  {
    
  }
  
  private static void p(String format, Object... args)
  {
    System.out.printf(format+"%n", args);
  }
}
