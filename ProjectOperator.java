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
  Project Operator implementation.

  This operator has exactly one child.

  The key parameters include:
    list of attributes to project on
    distinct or not (boolean varible)
    list of attributes to order by (null if no "order by" clause in the query)

  init(): no specific initialization required
  get_next(): return the next tuple
  close(): no specific close required
***************************************************************************************************/
public class ProjectOperator extends Operator {
    Operator child;
    Vector<TupleAttribute> select_attributes;
    boolean distinct;
    Vector<TupleAttribute> order_by_attributes;

    RelationSchema outputRelationSchema = null;

    ProjectOperator(Operator child, Vector<TupleAttribute> select_attributes, boolean distinct, Vector<TupleAttribute> order_by_attributes)
    {
        this.child = child;
        this.select_attributes = select_attributes;
        this.distinct = distinct;
        this.order_by_attributes = order_by_attributes;

        /* Create the corresponding output relation schema. */
        outputRelationSchema = new RelationSchema(select_attributes.size());

        for(int i = 0; i < select_attributes.size(); i++) {
            TupleAttribute ta = select_attributes.get(i);
            outputRelationSchema.setAttribute(i, ta.tableName + "." + ta.attributeName, ta.getAttributeType());
        }

        /* DISTINCT currently not implemented. */
        if(distinct) {
            System.out.println("DISTINCT not implemented.");
            System.exit(1);
        }
        if(order_by_attributes != null) {
            System.out.println("Asked to order by attributes: " + order_by_attributes + ", but ORDER BY not implemented.");
            System.exit(1);
        }
    }

    /* Pretty print. */
    void print(int num_tabs) {
        for(; num_tabs > 0; num_tabs--) 
            System.out.print("	");
        System.out.println("Project operator: on " + select_attributes + " with distinct = " + distinct + " and order by on: " + order_by_attributes);
        child.print(num_tabs+1);
    }


    void init() {
        child.init();
    }

    Tuple get_next() {
        Tuple in = child.get_next();

        if(in == null) {
            return null;
        } else {
            IntermediateFlattenedTuple out = new IntermediateFlattenedTuple(outputRelationSchema.getNumberOfAttributes());

            for(int i = 0; i < select_attributes.size(); i++) {
                out.setValue(i, select_attributes.get(i).evaluate(in));
            }

            return out;
        }
    }

    void close() {
        child.close();
    }
}
