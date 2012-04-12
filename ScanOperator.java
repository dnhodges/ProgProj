import com.sleepycat.je.Database;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.LockMode;  
import com.sleepycat.je.OperationStatus; 

import java.io.*;
import java.util.Vector;

/*****************************************************************************************************
  Sequential Scan Operator implementation.

  init(): open the database and set up the cursor.
  get_next(): get the next tuple from the table that satisfies the predicates.
  close(): close the cursor etc.

  We also need:
  re_init(): reinitialize the cursor and make it point to the beginning of table.
***************************************************************************************************/
public class ScanOperator extends Operator {
    /* Pre-init information. */
    BaseRelationSchema rs;
    Vector<Predicate> predicates;

    /* The Database, Cursor etc. */
    Database myDatabase = null;
    Cursor myCursor = null;

    ScanOperator(BaseRelationSchema rs, Vector<Predicate> predicates_vector) 
    {
        this.rs = rs;
        this.predicates = predicates_vector;
    }

    /* Pretty print. */
    void print(int num_tabs) {
        for(; num_tabs > 0; num_tabs--) 
            System.out.print("	");
        System.out.print("Scan operator on " + rs.getName());

        if(predicates.size() == 0) {
            System.out.println();
        } else {
            System.out.print(" with predicates: ");
            for(Predicate p : predicates)
                System.out.print((p == predicates.get(0) ? "": ", ") + p);
            System.out.println();
        }
    }

    BaseRelationSchema getRelationSchema() {
        return rs;
    }

    /* Open a cursor for reading the tuples. */
    void open_cursor() {
        try {
            /* Open a cursor. */
            if(myCursor != null) 
                myCursor.close();

            myCursor = myDatabase.openCursor(null, null);
        } catch (Exception dbe) {
            // Exception handling goes here
            dbe.printStackTrace();
            System.out.println("=========> Error during initialization of the scan operator");
            System.exit(1);
        }
    }


    void init() {
        /* Open the relation. */
        myDatabase = Globals.openDatabase(rs.getName());

        /* Open the cursor. */
        open_cursor();
    }

    /* We need this for doing nested loops join. */
    void re_init() {
        /* No need to open the database. It is already open. */
        /* Open the cursor. */
        open_cursor();
    }

    Tuple get_next() {
        try {
            // Cursors need a pair of DatabaseEntry objects to operate. These hold
            // the key and data found at any given position in the database.
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            // To iterate, just call getNext() until the last database record has been 
            // read. All cursor operations return an OperationStatus, so just read 
            // until we no longer see OperationStatus.SUCCESS
            while (myCursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                boolean satisfies = true;

                Tuple t = (Tuple) rs.getCustomBinding().entryToObject(foundData);

                /* Check if it satisfies the predicates. */
                for(Predicate p : predicates) {
                    if(! p.evaluate(t)) {
                        satisfies = false;
                        break;
                    }
                }

                if(satisfies) 
                    return t;
            }
        } catch (Exception dbe) {
            // Exception handling goes here
            dbe.printStackTrace();
            System.exit(1);
        }
        return null;
    }

    void close() {
        try {
            myCursor.close();
        } catch (Exception dbe) {
            // Exception handling goes here
            dbe.printStackTrace();
            System.out.println("=========> Error during closing of the scan operator");
            System.exit(1);
        }

        /* Close the relation. */
        Globals.closeDatabase(myDatabase);
    }
}

