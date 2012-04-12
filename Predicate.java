import java.util.Vector;

/**************************************************************************************************
  This file contains a few important classes.

  1. Variable: A variable may be a "Constant" (like 1 or 'XYZ') or a "TupleAttribute" (like "R.a" or "S.b").
    For both of them, a function evaluate(Tuple) is supported, which returns the 
    result of applying the variable to the tuple.
    For Constant: the result is always the constant value.
    For TupleAttribute: we find the value of the attribute for that tuple.

  2. Predicate:
        A predicate is of the type:
                Variable1 = Variable2

************************************************************************************************/
abstract class Variable {
    abstract Object evaluate(BaseTuple t);
    abstract Object evaluate(IntermediateTuple t);

    Object evaluate(Tuple t) {
        if(t instanceof BaseTuple) {
            return evaluate((BaseTuple) t);
        } else {
            return evaluate((IntermediateTuple) t);
        }
    }
}

class Constant extends Variable {
    Object o;

    Object evaluate(BaseTuple t) {return o; }
    Object evaluate(IntermediateTuple t) {return o; }

    Constant(Object o) {
        this.o = o;
    }

    public String toString() {
        return "Constant: " + o;
    }
}

class TupleAttribute extends Variable {
    String tableName;
    String attributeName;

    BaseRelationSchema rs;

    int position_of_attribute_in_table = -1;

    /* Careful --- t might be null. */
    TupleAttribute(String t, String a) {
        tableName = t;
        if(a.contains(tableName + ".")) 
            attributeName = a.replace(tableName + ".", "");
        else 
            attributeName = a;
    }

    void setRelationSchema(BaseRelationSchema rs) {
        this.rs = rs;
        position_of_attribute_in_table = this.rs.getPosition(attributeName);
    }

    int getAttributeType() {
        return rs.getAttributeType(rs.getPosition(attributeName));
    }

    BaseRelationSchema getRelationSchema() {
        return rs;
    }

    Object evaluate(BaseTuple t) {
        return t.getAttributeValueByPosition(position_of_attribute_in_table);
    }

    /* To "evaluate" on an intermediateTuple, we simply find the corresponding BaseTuple. */
    Object evaluate(IntermediateTuple t) {
        return evaluate(t.getBaseTuple(rs));
    }

    public String toString() {
        return tableName + "." + attributeName;
    }
}

public class Predicate {
    Variable v1;
    Variable v2;

    Predicate() {
        v1 = null;
        v2 = null;
    }

    Variable lhs() { return v1; }

    Variable rhs() { return v2; }

    BaseRelationSchema leftRelationSchema() {
        return ((TupleAttribute) v1).getRelationSchema();
    }

    BaseRelationSchema rightRelationSchema() {
        return ((TupleAttribute) v2).getRelationSchema();
    }

    /* We may need to swap the variables to fit with the query plan. */
    void swap() {
        Variable temp = v1;
        v1 = v2;
        v2 = temp;
    }

    void addVariable(Variable v) {
        if(v1 == null) {
            v1 = v;
        } else {
            assert v2 == null;
            v2 = v;
        }
    }

    Predicate(Variable v1, Variable v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    boolean isJoinPredicate()  {
        return v2 instanceof TupleAttribute;
    }

    boolean isScanPredicate(RelationSchema rs) {
        if(isJoinPredicate()) {
            return false;      
        } else {
            return ((TupleAttribute) v1).rs == rs;
        }
    }

    public String toString() {
        return v1 + " = " + v2;
    }

    boolean evaluate(Tuple t) {
        assert v2 instanceof Constant;
        return v1.evaluate(t).equals(v2.evaluate(t));
    }

    boolean evaluate(Tuple t1, Tuple t2) {
        return v1.evaluate(t1).equals(v2.evaluate(t2));
    }
}
