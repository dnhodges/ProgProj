import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.TupleInput;


import java.util.Hashtable;
import java.util.Vector;

/*****************************************************************************************************
  RelationSchema contains the description of the schema of a table. 

  A RelationSchema may corresopnd to a base relation (subclass: BaseRelationSchema) or to an intermediate
  output during query processing (the basic class).

  We use the RelationSchema objects as unique identifiers everywhere, so there should only be one 
  instance of a RelationSchema object corresponding to a relation.
  All RelationSchema objects are stored in a Hashtable in "Globals".
***************************************************************************************************/

public class RelationSchema {
    /* A schema is simply the list of attributes along with their types. */
    String[] attributeNames;
    int[] attributeTypes;

    RelationSchema() {
    }

    RelationSchema(int numAttributes) {
        attributeNames = new String[numAttributes];
        attributeTypes = new int[numAttributes];
    }

    void setAttribute(int index, String name, int type) {
        attributeNames[index] = name;
        attributeTypes[index] = type;
    }

    int getAttributeType(int i) {
        return attributeTypes[i];
    }

    int getNumberOfAttributes() {
        return attributeNames.length;
    }

    boolean hasAttribute(String attrName) {
        return getPosition(attrName) != -1;
    }

    int getPosition(String attrName) {
        for(int i = 0; i < attributeNames.length; i++) 
            if(attributeNames[i].equals(attrName)) 
                return i;
        return -1;
    }
}

/* A base relation schema also contains additional information like the name of the relation, the 
 * bindings needed to construct the tuples etc. */
class BaseRelationSchema extends RelationSchema {
    String relationName;
    int primaryKeyPosition = -1;

    /* This is needed for tuple input/output purposes. See Tuple.java for the definition. */
    RelationSpecificTupleBinding binding = null;

    RelationSpecificTupleBinding getCustomBinding() {
        if(binding == null) {
            binding = new RelationSpecificTupleBinding(this);
        }
        return binding;
    }

    /* Is the vector of values type-consistent with the schema ? */
    boolean checkIfConsistent(Vector<Object> values) {
        if(values.size() != getNumberOfAttributes()) 
            return false;

        for(int i = 0; i < values.size(); i++) {
            if( (getAttributeType(i) == Globals.STRING) && (values.get(i) instanceof Integer)) {
                System.out.println("=========> At position " + i + " expecting String, but got Integer");
                return false;
            }
            if( (getAttributeType(i) == Globals.INTEGER) && (values.get(i) instanceof String))  {
                System.out.println("=========> At position " + i + " expecting Integer, but got String");
                return false;
            }
        }

        return true;
    }

    BaseRelationSchema(String rName, int numAttributes, int pk_position) {
        super(numAttributes);
        relationName = rName;
        primaryKeyPosition = pk_position;
    }

    BaseRelationSchema(String rName, int numAttributes) {
        super(numAttributes);
        relationName = rName;
    }

    String getName() {
        return relationName;
    }

    int getPrimaryKeyPosition() {
        return primaryKeyPosition;
    }

    int getPosition(String attrName) {
        // Thread.dumpStack();
        String searchfor = relationName + "." + attrName;
        for(int i = 0; i < attributeNames.length; i++)  { 
            // System.out.println("Comparing " + attributeNames[i] + " with " + searchfor);
            if(attributeNames[i].equals(searchfor)) 
                return i;
        }
        return -1;
    }
}


/**********************************************************************************************
  Since we store the RelationSchemas in a Database (called "metadata"), we need a way 
  to serialize and de-serialize these. 

  Read the BerkeleyDB Java API Description for TupleBindings before trying to understand this.
***********************************************************************************************/
class RelationSchemaTupleBinding extends TupleBinding {
    RelationSchemaTupleBinding() {
    }

    // Write a RelationSchema object to a TupleOutput
    public void objectToEntry(Object object, TupleOutput to) {
        // Write the data to the TupleOutput (a DatabaseEntry).
        // Order is important. The first data written will be
        // the first bytes used by the default comparison routines.

        BaseRelationSchema rs = (BaseRelationSchema) object;

        to.writeString(rs.relationName);
        to.writeInt(rs.attributeNames.length);
        to.writeInt(rs.primaryKeyPosition);

        for(int i = 0; i < rs.attributeNames.length; i++) {
            to.writeString(rs.attributeNames[i]);
            to.writeInt(rs.attributeTypes[i]);
        }
    }

    // Convert a TupleInput to a RelationSchema object
    public Object entryToObject(TupleInput ti) {
        String relationName = ti.readString();
        int num_attributes = ti.readInt();
        int pk_position = ti.readInt();

        BaseRelationSchema rs = new BaseRelationSchema(relationName, num_attributes, pk_position);

        for(int i = 0; i < num_attributes; i++) {
            rs.setAttribute(i, ti.readString(), ti.readInt());
        }

        return rs;
    }
}
