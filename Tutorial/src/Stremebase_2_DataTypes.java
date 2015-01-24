import java.util.Scanner;

import com.stremebase.map.OneMap;
import com.stremebase.base.DB;
import com.stremebase.base.To;


public class Stremebase_2_DataTypes
{
  public static OneMap map;
  
  private static final Scanner in = new Scanner(System.in);
  
  
  public static void main(String[] args)
  {
    DB.startDB(false);
    map = new OneMap("map");
    welcome();
    lesson1_integers();
    lesson2_booleans();
    /*lesson3_doubles();
    lesson4_temporalunits();
    lesson5_strings();*/
    l();
    p("Bye!");
  }
  
  public static void welcome()
  {
    l();
    p("Stremebase v.0.1 Tutorial - Chapter 2: Supported Data Types");
    p("");
    p("Under the hood Stremebase operates with longs (arrays of 64 bits).");
    p("Therefore any data type for which you can devise a bijection to longs is supported.");
    p("And if the bijection is order-preserving, even index-based queries will work correctly.");
    p("");
    p("For your convenience, there is a class called com.stremebase.base.To");
    p("It contains static functions for converting most common datatypes to longs and back.");
    p("Therefore following data types are supported out of the box:");
    p("");
    p("long: naturally - hence anything Java can cast to long: ints, shorts, bytes, ...");
    p("boolean: you get not one, but 64 booleans per entry!");
    p("double: floating point numbers");
    p("java.time.Instant: with a millisecond precision");
    p("java.time.localDateTime: Instant with a zone information from DB.db.ZONE");
    p("java.lang.String: no size limits");
    p("java.lang.StringBuilder: full text documents can be indexed and searched");
    p("");
    p("If you cannot figure out how to map a datatype to long (some BLOB maybe),");
    p("you can create a map with serializable objects as values,");
    p("but then indexing is not supported.");
    p("");
    p("Without further ado, let's try this out in practice.");
    p("");
    p("(Press ENTER when ready...)");
    in.nextLine();
  }
  
  public static void lesson1_integers()
  {    
    l();
    p("1: INTEGER DATA TYPES");
    p("");
    p("Here are examples of how you handle integers (whole numbers): ");
    p("");
    p("long lv = 4564564565234l;");
    p("map.put(1, lv);");
    p("lv = map.get(1);");
    p("map.commit();");
    p("System.out.printf(\"%%d\", lv);");
    long lv = 4564564565234l;
    map.put(1, lv);
    map.commit();
    lv = map.get(1);
    System.out.printf("%d", lv);
    p("");
    p("");
    p("int iv = 22222222;");
    p("map.put(2, iv);");
    p("map.commit();");
    p("iv = map.get(2);");
    p("System.out.printf(\"%%d\", iv);");
    long iv = 22222222;
    map.put(2, iv);
    map.commit();
    iv = map.get(2);
    System.out.printf("%d", iv);
    p("");
    p("");
    p("short sv = -32333;");
    p("map.put(3, sv);");
    p("map.commit();");
    p("sv = (short) map.get(3);");
    p("System.out.printf(\"%%d\", sv);");
    short sv = -32333;
    map.put(3, sv);
    map.commit();
    sv = (short) map.get(3);
    System.out.printf("%d", sv);
    p("");
    p("");
    p("char cv = 'A';");
    p("map.put(4, cv);");
    p("map.commit();");
    p("cv = (char) map.get(4);");
    p("System.out.printf(\"%%c\", cv);");
    char cv = 'A';
    map.put(4, cv);
    map.commit();
    cv = (char) map.get(4);
    System.out.printf("%c", cv);
    p("");
    p("");
    p("byte bv = 55;");
    p("map.put(5, bv);");
    p("map.commit();");
    p("bv = (byte) map.get(5);");
    p("System.out.printf(\"%%d\", bv);");
    byte bv = 55;
    map.put(5, bv);
    map.commit();
    bv = (byte) map.get(5);
    System.out.printf("%d", bv);
    p("");
    in.nextLine();      
  }
  
  public static void lesson2_booleans()
  {
    l();
    p("2: BOOLEANS");
    p("");
    p("Because 64 bits is smallest addressable unit, we can handle 63 booleans in one shot:");
    p("(you'd better not tamper with the 63rd bit)");
    p("");
    
    p("long booleanArray = 0;");
    p("boolean b0 = true;");
    p("boolean b1 = false;");
    p("boolean b62 = true;");
    p("map.put(1, booleanArray=To.l(b0, 0, booleanArray));");
    p("map.put(1, booleanArray=To.l(b1, 1, booleanArray));");
    p("map.put(1, booleanArray=To.l(b62, 62, booleanArray));");
    p("map.commit();");
    p("b0 = To.toBoolean(map.get(1), 0);");
    p("b1 = To.toBoolean(map.get(1), 1);");
    p("b62 = To.toBoolean(map.get(1), 62);");
    p("System.out.printf(\"%%b, %%b, %%b\", b0, b1, b62);");
   
    map.clear();
    long booleanArray = 0;
    boolean b0 = true;
    boolean b1 = false;
    boolean b62 = true;
    map.put(1, booleanArray=To.l(b0, 0, booleanArray));
    map.put(1, booleanArray=To.l(b1, 1, booleanArray));
    map.put(1, booleanArray=To.l(b62, 62, booleanArray));
    map.commit();
    b0 = To.toBoolean(map.get(1), 0);
    b1 = To.toBoolean(map.get(1), 1);
    b62 = To.toBoolean(map.get(1), 62);
    System.out.printf("%b, %b, %b", b0, b1, b62);
    p("");
    p("");
    p("For these boolean arrays, range queries do not make sense.");
    p("Instead, query by scanning through all values and applying a bit mask.");
    p("For example: to get keys where 0th and 62nd booleans are true, you would: ");
    p("final long trueMask = To.mask(0, 62);");    
    p("map.keyset().filter(key -> ((map.get(key) & trueMask) == trueMask)).forEach(key -> p(\"%%d\", key));");
    final long trueMask = To.mask(0, 62);    
    map.keyset().filter(key -> ((map.get(key) & trueMask) == trueMask)).forEach(key -> p("%d", key));
    p("");
    p("And to get keys where 1st boolean is false, you would: ");
    p("final long falseMask = To.mask(1);");    
    p("map.keyset().filter(key -> ((map.get(key) & falseMask) == 0)).forEach(key -> p(\"%%d\", key));");
    final long falseMask = To.mask(1);    
    map.keyset().filter(key -> ((map.get(key) & falseMask) == 0)).forEach(key -> p("%d", key));
    
    in.nextLine();
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
