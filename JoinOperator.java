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

/******************** 
  This is the abstract JoinOperator class.
  A JoinOperator has two children: a left child and a right child. 
  It also has an associated Predicate.
  *******************/

public abstract class JoinOperator extends Operator {
    Operator leftOp;
    Operator rightOp;

    Predicate jp;

    JoinOperator(Operator l, Operator r, Predicate jp) {
        this.leftOp = l;
        this.rightOp = r;
        this.jp = jp;
    }

    /* Pretty print for the query plan. */
    void print(int num_tabs) {
        for(int i = 0; i < num_tabs; i++) 
            System.out.print("	");
        System.out.println("Join operator with predicate " + jp);
        leftOp.print(num_tabs+1);
        rightOp.print(num_tabs+1);
    }


    Operator getLeftOperator() {
        return leftOp;
    }

    Operator getRightOperator() {
        return rightOp;
    }

    void init() {
        leftOp.init();
        rightOp.init();
    }

    boolean checkPredicateSatisfied(Tuple left, Tuple right) {
        return true;
    }

    void close() {
        leftOp.close();
        rightOp.close();
    }

    /* We will use the HashJoinOperator if we can use it, otherwise we use the NestedLoopsJoinOperator. */
    static JoinOperator createNewJoinOperator(Operator left, Operator right, Predicate jp) {
        if(HashJoinOperator.canBeUsed(left, right, jp)) {
            return new HashJoinOperator(left, right, jp);
        } else {
            /* Nested Loops can always be used. */
            return new NestedLoopsJoinOperator(left, right, jp);
        }
    }
}
