import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.bind.EntryBinding;

import java.io.*;

/***************************************************************************************************************************
  The CommandLine class provides a command line interface to the database (this is the only available interface
  at this time). 

  It reads in SQL statements one by one, and executes them. 
  ***************************************************************************************************************************/
public class CommandLine {

    /* Create a table. Error if it already exists. 
       The Globals.createDatabase() call actually creates a BerkeleyDB "Database".
       We also need to update the in-memory cache of all RelationSchema's (also 
        stored in Globals).
     */
    public static void create_new_table(ParsedStatement ps) 
    {
        if(Globals.existsRelation(ps.ct_tableName)) {
            System.out.println("=========> The table " + ps.ct_tableName + " already exists.");
        } else {
            BaseRelationSchema rs = new BaseRelationSchema(ps.ct_tableName, ps.ct_attributeNames.size(), ps.ct_primaryKeyPosition);
            for(int i = 0; i < ps.ct_attributeNames.size(); i++) 
                rs.setAttribute(i, ps.ct_attributeNames.get(i).replace("null", ps.ct_tableName), ps.ct_attributeTypes.get(i).intValue());

            /* Potential for inconsistencies here since the first statement may execute, and second may not. 
               We won't worry about it. */
            Globals.addNewRelationSchema(rs);
            Globals.createDatabase(ps.ct_tableName);
        }
    }

    /* Drop a table. Error if it already exists. 
       Reverse the above stpes. */
    public static void drop_table(ParsedStatement ps) {
        if(! Globals.existsRelation(ps.dt_tableName)) {
            System.out.println("=========> The table " + ps.dt_tableName + " does not exist.");
        } else {
            /* Potential for inconsistencies here because the first statement may execute and the second may not.
               We won't worry about it. */
            Globals.removeRelationSchema(Globals.getRelationSchema(ps.dt_tableName));
            Globals.removeDatabase(ps.dt_tableName);
        }
    }

    /* Insert a new tuple into a relation. */
    public static void insert_values(ParsedStatement ps) {
        // First find the corresponding relation schema and make sure it exists.
        BaseRelationSchema rs = Globals.getRelationSchema(ps.iv_tableName);

        if(rs == null) {
            System.out.println("=========> Table " + ps.iv_tableName + " does not exist");
        } else {
            /* Next check that the insert values are consistent with the relation schema. */
            if(! rs.checkIfConsistent(ps.iv_attributeValues)) {
                System.out.println("=========> Error: Values do not match the relation schema.");
            } else {
                Globals.insertTuple(rs, new BaseTuple(rs, ps.iv_attributeValues));
            }
        }
    }

    /* Execute a "select" query. Most of the logic is in the Query class. */
    public static void execute_query(ParsedStatement ps) {
        Query q = new Query(ps);

        if(q.analyze() && q.plan()) {
            q.print();
            q.executeQuery();
        }
    }

    public static void main(String argv[]) {
        // Open the database using the argv arguments
        Globals.initialize(argv[0]);

        // Start reading the commands
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        System.out.println();
        System.out.print("command> ");

        try {
            String str = "";
            while((str = in.readLine()) != null) {
                ParsedStatement ps = SQLParser.Parse(str);
                if(ps != null) {
                    ps.print();

                    switch(ps.statementType) {
                        case ParsedStatement.CREATE_TABLE:
                            create_new_table(ps);
                            break;

                        case ParsedStatement.INSERT_VALUES:
                            insert_values(ps);
                            break;

                        case ParsedStatement.QUERY:
                            execute_query(ps);
                            break;

                        case ParsedStatement.DROP_TABLE:
                            drop_table(ps);
                            break;

                        default:
                            assert false : "This shouldn't happen";
                    }
                }

                System.out.println();
                System.out.print("command> ");
            }

            Globals.close();

        } catch (Exception e) {
            System.out.println("Non-fatal exception: " + e);
            e.printStackTrace();
        }
    }
}
