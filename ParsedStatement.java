import java.util.Vector;

/*********************************************************************************************
  This Class simply stores the information extracted during the parsing. 
  
  We currently support four types of statements:
  1. CREATE_TABLE
  2. INSERT_VALUES
  3. QUERY 
    Here we only support a very limited subset of SQL select-project-join queries.
  4. DROP_TABLE

  For each of them, the information is extracted during the parsing.
 *********************************************************************************************/
public class ParsedStatement {
    static final int CREATE_TABLE = 1;
    static final int INSERT_VALUES = 2;
    static final int QUERY = 3;
    static final int DROP_TABLE = 4;

    int statementType = 0;

    /* Create Table Statement and its Parameters. */
    String ct_tableName;
    Vector<String> ct_attributeNames = new Vector<String>();        // Attribute Names
    Vector<Integer> ct_attributeTypes = new Vector<Integer>();;     // Attribute Types -- String or Integer
    int ct_primaryKeyPosition = -1;

    void initCreateTable(String s) {
      statementType = CREATE_TABLE;
      ct_tableName = s;
    }

    void addTableAttribute(String s, String type, boolean isPrimaryKey) {
        if(isPrimaryKey) {
            assert ct_primaryKeyPosition == -1;
            ct_primaryKeyPosition = ct_attributeNames.size();
        }
        ct_attributeNames.add(ct_tableName + "." + s);
        if(type.equals("string")) {
            ct_attributeTypes.add(new Integer(Globals.STRING));
        } else {
            assert type.equals("integer");
            ct_attributeTypes.add(new Integer(Globals.INTEGER));
        }
    }

    /* Drop Table Statement and its Parameters. */
    String dt_tableName;

    void initDropTable(String s) {
      statementType = DROP_TABLE;
      dt_tableName = s;
    }

    /* Insert Statement Values. */
    String iv_tableName;
    Vector<Object> iv_attributeValues = new Vector<Object>();;

    void initInsertValues(String s) {
      statementType = INSERT_VALUES;
      iv_tableName = s;
    }

    void addNumberAttributeValue(String s) {
        addAttributeValue(new Integer(s));
    }

    void addStringAttributeValue(String s) {
        addAttributeValue(s);
    }

    void addAttributeValue(Object o) { 
        iv_attributeValues.add(o);
    }

    /* Query. */
    Vector<String> fromTables = new Vector<String>();
    Vector<Predicate> wherePredicates = new Vector<Predicate>();
    Vector<TupleAttribute> selectAttributes = new Vector<TupleAttribute>();
    boolean distinct = false;
    Vector<TupleAttribute> orderByAttributes = null;

    void initQuery() {
      statementType = QUERY;
    }

    void initQueryDistinct() {
      statementType = QUERY;
      distinct = true;
    }

    void addQueryTable(String s) { 
        fromTables.add(s);
    }

    void addPredicate(Predicate pr) {
        wherePredicates.add(pr);
    }

    void addSelectVariable(TupleAttribute ta) {
        selectAttributes.add(ta);
    }
    
    void addOrderByAttribute(TupleAttribute ta) {
        if(orderByAttributes == null)
            orderByAttributes = new Vector<TupleAttribute>();
        orderByAttributes.add(ta);
    }

    /* Print the parsed statement. */
    void print() {
        switch(statementType) {
            case CREATE_TABLE:
                System.out.println("Creating table " + ct_tableName + " with attributes : " + ct_attributeNames + " of types : " + ct_attributeTypes);
                break;
            case INSERT_VALUES:
                System.out.println("Inserting into table " + iv_tableName + " values : " + iv_attributeValues);
                break;
            case QUERY:
                System.out.print("Query: on tables " + fromTables + " with predicates: " + wherePredicates + " selecting ");
                if(distinct) System.out.print("distinct ");
                System.out.print(": " + selectAttributes);
                if(orderByAttributes != null) 
                    System.out.println(" ordering by " + orderByAttributes);
                else
                    System.out.println();
                break;
            case DROP_TABLE:
                System.out.println("Dropping table " + dt_tableName);
                break;
            default:
                assert false : "This shouldn't happen";
        }
    }
}
