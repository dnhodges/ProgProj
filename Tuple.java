import java.util.Vector;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

/*****************************************************************
  Tuple classes. There are two types of tuples:

  1. BaseTuple:
        Specified by a RelationSchema object, and a list of values. 

  2. IntermediateTuple:
        Implemented as a vector of BaseTuples. 
*****************************************************************/
public class Tuple {
    void print() {
        System.out.println(this);
    }


    public String toString() {
        return "";
    }
}

class BaseTuple extends Tuple {
    BaseRelationSchema r;
    Object[] values;

    BaseTuple() {
        r = null;
        values = null;
    }

    BaseTuple(BaseRelationSchema r) {
        this.r = r;
        values = new Object[r.getNumberOfAttributes()];
    }

    BaseTuple(BaseRelationSchema r, Vector<Object> attributeValues) {
        this.r = r;
        values = new Object[r.getNumberOfAttributes()];

        for(int i = 0; i < values.length; i++) {
            setAttributeValueByPosition(i, attributeValues.get(i));
        }
    }

    BaseRelationSchema getRelationSchema() {
        return r;
    }

    Object getAttributeValueByPosition(int position) {
        return values[position];
    }

    Object getAttributeValueByName(String attrName) {
        return values[r.getPosition(attrName)];
    }

    void setAttributeValueByPosition(int position, Object o) {
        values[position] = o;
    }

    void setAttributeValueByName(String attrName, Object o) {
        values[r.getPosition(attrName)] = o;
    }

    Object getPrimaryKeyValue() {
        return values[r.getPrimaryKeyPosition()];
    }

    DatabaseEntry primaryKeyToEntry() {
        return Globals.simpleObjectToEntry(getPrimaryKeyValue());
    }

    public String toString() {
        String ret = " [";
        for(int i = 0; i < values.length; i++)
            ret += (i == 0 ? "" : "		") + values[i];
        return ret + "] ";
    }
}

/* An intermediate flattened tuple is simply a list of values. */
class IntermediateFlattenedTuple extends Tuple {
    Object[] values;

    IntermediateFlattenedTuple(int num_attributes) {
        values = new Object[num_attributes];
    }

    void setValue(int i, Object o) {
        values[i] = o;
    }

    public String toString() {
        String ret = "";
        for(int i = 0; i < values.length; i++)
            ret += (i == 0 ? "" : "		") + values[i];
        return ret;
    }
}

/* An intermediate (non-flattened) tuple is stored as a vector of basetuples. Ideally such representations 
 * should not be used, but it simplifies coding in many cases. */
class IntermediateTuple extends Tuple {
    Vector<BaseTuple> baseTuples = new Vector<BaseTuple>();

    BaseTuple getBaseTuple(RelationSchema rs) {
        for(BaseTuple bt : baseTuples) {
            if(bt.getRelationSchema() == rs) {
                return bt;
            }
        }
        return null;
    }

    /* Intermediate tuple constructed by combining two tuples. */
    IntermediateTuple(Tuple t1, Tuple t2) {

        if(t1 instanceof BaseTuple) {
            baseTuples.add((BaseTuple) t1);
        } else {
            for(BaseTuple bt : ((IntermediateTuple) t1).baseTuples) {
                baseTuples.add(bt);
            }
        }

        if(t2 instanceof BaseTuple) {
            baseTuples.add((BaseTuple) t2);
        } else {
            for(BaseTuple bt : ((IntermediateTuple) t2).baseTuples) {
                baseTuples.add(bt);
            }
        }
    }

    public String toString() {
        String ret = "Intermediate Tuple:  ";
        for(BaseTuple bt : baseTuples) {
            ret += bt.getRelationSchema().getName() + bt + "  ";
        }
        return ret;
    }
}

/**********************************************************************************************
  Read the BerkeleyDB Java API Description for TupleBindings before trying to understand this.
  This is essentially a way to serialize and deserialize Tuple objects.
***********************************************************************************************/
class RelationSpecificTupleBinding extends TupleBinding {
    BaseRelationSchema r;

    RelationSpecificTupleBinding(BaseRelationSchema r) {
        this.r = r;
    }

    // Write a Tuple object to a TupleOutput
    public void objectToEntry(Object object, TupleOutput to) {
        // Write the data to the TupleOutput (a DatabaseEntry).
        // Order is important. The first data written will be
        // the first bytes used by the default comparison routines.

        BaseTuple bt = (BaseTuple) object;

        for(int i = 0; i < r.getNumberOfAttributes(); i++) {
            if(r.getAttributeType(i) == Globals.INTEGER) {
                to.writeInt(((Integer) bt.values[i]).intValue());
            } else {
                assert r.getAttributeType(i) == Globals.STRING;
                to.writeString((String) bt.values[i]);
            }
        }
    }

    // Convert a TupleInput to a MyData2 object
    public Object entryToObject(TupleInput ti) {
        BaseTuple bt = new BaseTuple(r);

        for(int i = 0; i < r.getNumberOfAttributes(); i++) {
            if(r.getAttributeType(i) == Globals.INTEGER) {
                bt.setAttributeValueByPosition(i, new Integer(ti.readInt()));
            } else {
                assert r.getAttributeType(i) == Globals.STRING;
                bt.setAttributeValueByPosition(i, ti.readString());
            }
        }

        return bt;
    }
}
