import com.sleepycat.je.Database;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.LockMode;  
import com.sleepycat.je.OperationStatus; 
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;

import java.util.Hashtable;
import java.util.Vector;
import java.io.*;


/* This class stores the global information like the environment, the relation schemas etc. */
public class Globals {

    /*********************************
      Constants
      ********************************/
    public static final int INTEGER = 1;
    public static final int STRING = 2;

    /********************************
      The Environment
      ******************************/
    static Environment myDbEnvironment = null;

    static void initialize(String directory) {
        try {
            // Open the environment. Create it if it does not already exist.
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);

            myDbEnvironment = new Environment(new File(directory), envConfig);

            readRelationSchemas();

        } catch (Exception dbe) {
            System.out.println("=========> Error during initialization: " + dbe);
            dbe.printStackTrace();
            System.exit(1);
        }
    }

    static void close() {
        try {
            if (myDbEnvironment != null)
                myDbEnvironment.close();
        } catch (Exception dbe) {
            System.out.println("=========> Error during finishing up: " + dbe);
            dbe.printStackTrace();
            System.exit(1);
        }
    }


    /********************************
      Open a Database (basically a "table" in our normal parlance).
      *******************************/
    static Database openDatabase(String name, boolean allowCreate) {
        try {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(allowCreate);
            return myDbEnvironment.openDatabase(null, name, dbConfig);
        } catch (Exception dbe) {
            // Exception handling goes here
            System.out.println("=========> Error opening the database:" + dbe);
            dbe.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    static Database openOrCreateDatabase(String name) {
        return openDatabase(name, true);
    }

    static void createDatabase(String name) {
        try {
            Database myDatabase = openDatabase(name, true);
            myDatabase.close();
        } catch (Exception dbe) {
            // Exception handling goes here
            System.out.println("=========> Could not create the database");
            dbe.printStackTrace();
            System.exit(1);
        }
    }

    static Database openDatabase(String name) {
        return openDatabase(name, false);
    }

    static void closeDatabase(Database db) {
        try {
            db.close();
        } catch (Exception dbe) {
            // Exception handling goes here
            System.out.println("=========> Error during closing the database");
            dbe.printStackTrace();
            System.exit(1);
        }
    }

    static void removeDatabase(String dbName) {
        try {
            myDbEnvironment.removeDatabase(null, dbName);
        } catch (Exception dbe) {
            // Exception handling goes here
            System.out.println("=========> Error dropping the relation");
            dbe.printStackTrace();
            System.exit(1);
        }
    }

    /***********************************************************
      Insert tuple into a table using its primary key as the key. 
      **********************************************************/
    static void insertTuple(BaseRelationSchema rs, BaseTuple bt) {
        try {
            Database myDatabase = Globals.openDatabase(rs.getName());

            assert myDatabase != null; // There should not be any inconsistency between our RelationSchemas Hashtable and the database

            DatabaseEntry myData = new DatabaseEntry();
            rs.getCustomBinding().objectToEntry(bt, myData);

            DatabaseEntry myKey = bt.primaryKeyToEntry();

            myDatabase.put(null, myKey, myData);

            Globals.closeDatabase(myDatabase);
        } catch (Exception dbe) {
            System.out.println("=========> BerkeleyDB error while inserting... something must be seriously wrong. Bailing out.");
            System.out.println(dbe);
            dbe.printStackTrace();
            System.exit(1);
        }
    }

    /********************************
      RELATION SCHEMAS
      ******************************/
    /* At the beginning, we will read in all RelationSchemas from the "metadata" file. 
       We will store it as a HashTable. */
    static Hashtable allRelationSchemas = null;

    static boolean existsRelation(String name) {
        return allRelationSchemas.containsKey(name);
    }

    static BaseRelationSchema getRelationSchema(String name) {
        return (BaseRelationSchema) allRelationSchemas.get(name);
    }

    static void readRelationSchemas() {
        try {
            allRelationSchemas = new Hashtable();

            Database myDatabase = openOrCreateDatabase("metadata"); 

            RelationSchemaTupleBinding rstb = new RelationSchemaTupleBinding();

            /* Read all the relationschemas and insert into the HashTable. */
            Cursor myCursor = myDatabase.openCursor(null, null);

            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                BaseRelationSchema rs = (BaseRelationSchema) rstb.entryToObject(foundData);
                allRelationSchemas.put(rs.relationName, rs);
            }

            myCursor.close();

            closeDatabase(myDatabase);
        } catch (Exception dbe) {
            // Exception handling goes here
            System.out.println("=========> Error: " + dbe);
            dbe.printStackTrace();
            System.exit(1);
        }
    }

    /* This statement is essentially executed as a result of the "create table" command. */
    static void addNewRelationSchema(BaseRelationSchema rs) {
        Database myDatabase = openDatabase("metadata"); 
        RelationSchemaTupleBinding rstb = new RelationSchemaTupleBinding();

        try {
            /* Serialize and write the RelationSchema object in there. */
            DatabaseEntry myData = new DatabaseEntry();
            rstb.objectToEntry(rs, myData);

            DatabaseEntry myKey = simpleObjectToEntry(rs.relationName); // Don't really care.

            myDatabase.put(null, myKey, myData);

            closeDatabase(myDatabase);

            /* Also add the relation schema to the in-memory hash table. */
            allRelationSchemas.put(rs.relationName, rs);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /* This statement is essentially executed as a result of the "drop table" command. */
    static void removeRelationSchema(BaseRelationSchema rs) {
        Database myDatabase = openDatabase("metadata"); 

        try {
            DatabaseEntry myKey = simpleObjectToEntry(rs.relationName); 

            myDatabase.delete(null, myKey);

            closeDatabase(myDatabase);

            /* Also add the relation schema to the in-memory hash table. */
            allRelationSchemas.remove(rs.relationName);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    /*****************************
      Some Utilities.
      ***************************/
    static DatabaseEntry simpleObjectToEntry(Object o) {
        try {
            if(o instanceof String) {
                return new DatabaseEntry(((String) o).getBytes("UTF-8"));
            } else {
                assert o instanceof Integer;
                DatabaseEntry de = new DatabaseEntry();
                EntryBinding myBinding = TupleBinding.getPrimitiveBinding(Integer.class);
                myBinding.objectToEntry(o, de);
                return de;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }

}
