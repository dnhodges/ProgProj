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
  The abstract Operator class. 
  Specifies three functions to be implemented for the get_next() iterator interface.

  init(): initialize the operator state
  Tuple get_next(): get the next output tuple
  close(): clean up.
***************************************************************************************************/
public abstract class Operator {
    static final int SCAN = 1;
    static final int JOIN = 2;

    int operator_type = 0;

    abstract void init();

    abstract Tuple get_next();

    abstract void close();

    void print() {
        print(0);
    }

    abstract void print(int num_tabs);
}
