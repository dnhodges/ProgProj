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
import java.util.Hashtable;

/****************************************************************************************************
  This class implements the NestedLoops Join.

  At any point: 
    We have a current left tuple, and a cursor into the right table.

  When get_next() is called:
    We continue iterating through the right table till another match is found, and we return it.
    If no match is found, we advance the left pointer (getting a new leftTuple), 
        reinitialize the cursor on the right table to the beginning, and
        repeat.
******************************************************************************************************/

public class NestedLoopsJoinOperator extends JoinOperator {
    /* Nested Loops join can always be used, and is currently implemented. */
    static boolean canBeUsed(Operator left, Operator right, Predicate jp) {
        return true;
    }

    void init() {
        super.init();

        /* Make sure that the rightOp is a ScanOperator. */
        assert rightOp instanceof ScanOperator;
    }

    NestedLoopsJoinOperator(Operator l, Operator r, Predicate jp) {
        super(l, r, jp);
    }

    /* Pretty print for the query plan. */
    void print(int num_tabs) {
        for(int i = 0; i < num_tabs; i++) 
            System.out.print("	");
        System.out.println("Nested Loops Join operator with predicate " + jp);
        leftOp.print(num_tabs+1);
        rightOp.print(num_tabs+1);
    }


    Tuple leftTuple = null;

    Tuple get_next() {
        /* If there are still tuples remaining corresponding to the current left tuple, create and return an intermediate tuple.
           Else get the next left tuple. */

        while(true) {
            if(leftTuple == null) { 
                leftTuple = leftOp.get_next();

                if(leftTuple == null) 
                    return null;
            }

            Tuple rightTuple = null;
            while((rightTuple = rightOp.get_next()) != null) {
                if(jp.evaluate(leftTuple, rightTuple))
                    return new IntermediateTuple(leftTuple, rightTuple);
            }

            /* No matching tuples left. Reinitialize the right scan. Set the leftTuple = null. */
            leftTuple = null;
            ((ScanOperator) rightOp).re_init();
        }
    }

    /* Don't need to do anything special. We will close the cursors on the tables in the ScanOpeartor.close(). */
    void close() {
        super.close();
    }
}
