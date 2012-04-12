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

public class HashJoinOperator extends JoinOperator {
    /* HashJoinOperator can typically only be used if the Join Predicate is an equality. 
       For us, since we only deal with equality predicates, this is not an issue.
       We should also worry about whether the "right" relation will fit in memory, but
       we will ignore that for now.

       NOTE: SET THIS TO RETURN "TRUE" AFTER YOU IMPLEMENT THE OPERATOR. */
    static boolean canBeUsed(Operator left, Operator right, Predicate jp) {
        return false;
    }

    void init() {
        assert false : "Hash Join Not Implemented Yet";
    }

    HashJoinOperator(Operator l, Operator r, Predicate jp) {
        super(l, r, jp);
    }

    /* Pretty print for the query plan. */
    void print(int num_tabs) {
        for(int i = 0; i < num_tabs; i++) 
            System.out.print("	");
        System.out.println("Hash Join operator with predicate " + jp);
        leftOp.print(num_tabs+1);
        rightOp.print(num_tabs+1);
    }

    Tuple get_next() {
        assert false : "Hash Join Not Implemented Yet";
        return null;
    }

    void close() {
        assert false : "Hash Join Not Implemented Yet";
    }
}
