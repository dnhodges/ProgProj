import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.bind.EntryBinding;

import java.io.*;
import java.util.Vector;

/*****************************************************************************************************
  This class contains the actual query evaluation and planning logic. 

  We currently only support queries of the form:
    select [distinct] <list of attributes>
    from <list of tables>
    where <list of predicates>
    order by <list of attributes>;

  The Select Clause can either contain "*" or a list of fully defined attributes.
  No aliasing is allowed. 
  Only equality predicates are allowed.

  Queries should not contain cycles or should not require Cartesian products.
***************************************************************************************************/

public class Query {
    /* All the information about the query. */
    Vector<BaseRelationSchema> query_relations;
    Vector<String> query_relation_names;
    Vector<Predicate> query_predicates;
    Vector<TupleAttribute> select_attributes;
    boolean distinct;
    Vector<TupleAttribute> order_by_attributes;

    /* The root operator. We will execute the query by doing a get_next() on the root. */
    Operator root;

    /* Execute the query. */
    void executeQuery() {
        /* Initialize. root will recursively call init on its children. */
        root.init();

        /* Print out the select attributes. */
        System.out.println("-------------------------------------------------------------------------");
        for(TupleAttribute attr : select_attributes) 
            System.out.print(attr + "		");
        System.out.println();
        System.out.println("-------------------------------------------------------------------------");

        /* Print out the result tuples one by one. */
        Tuple t = null;
        while((t = root.get_next()) != null) {
            System.out.println(t);
        }
        System.out.println("-------------------------------------------------------------------------");

        /* Close. */
        root.close();
    }

    Query(ParsedStatement ps) {
        query_relation_names = ps.fromTables;
        query_predicates = ps.wherePredicates;
        select_attributes = ps.selectAttributes;
        distinct = ps.distinct;
        order_by_attributes = ps.orderByAttributes;
    }

    /** Is the argument relation contained in the From Clause ? **/
    boolean checkRelationContainedInFromClause(String name) {
        for(BaseRelationSchema rs : query_relations) 
            if(rs.getName().equals(name))
                return true;
        return false;
    }

    /** Check if a tuple attribute reference is valid. 
        Disambiguate if needed. Also set the RelationSchema for the TupleAttribute. **/
    boolean analyzeTupleAttribute(TupleAttribute ta) {
        if(ta.tableName == null) {
            BaseRelationSchema found_in = null;
            for(BaseRelationSchema rs : query_relations) {
                // System.out.println("searching for " + ta.attributeName + " in " + rs);
                if(rs.hasAttribute(ta.attributeName)) {
                    if(found_in == null) {
                        found_in = rs;
                    } else {
                        System.out.println("=========> Attribute " + ta.attributeName + " ambiguous");
                        return false;
                    }
                }
            }

            if(found_in != null) {
                ta.tableName = found_in.getName();
                ta.setRelationSchema(found_in);
                return true;
            } else {
                System.out.println("=========> Attribute " + ta.attributeName + " not found in any of the tables in the FROM clause");
                return false;
            }
        } else {
            if(! checkRelationContainedInFromClause(ta.tableName)) {
                System.out.println("=========> Relation " + ta.tableName + " not in the From Clause");
                return false;
            }
            ta.setRelationSchema(Globals.getRelationSchema(ta.tableName));
            if(! ta.rs.hasAttribute(ta.attributeName)) {
                System.out.println("=========> Attribute " + ta.attributeName + " not present in the relation " + ta.tableName);
                return false;
            }
            return true;
        }
    }

    /* Construct the query object. Analyze, check for errors etc. */
    boolean analyze() {
        /* Get the relation schemas, and make sure the relations exist. */
        query_relations = new Vector<BaseRelationSchema>();

        for(String name : query_relation_names) {
            BaseRelationSchema rs;
            if( (rs = Globals.getRelationSchema(name)) == null) {
                System.out.println("=========> Relation " + name + " does not exist");
                return false;
            }
            query_relations.add(rs);
        }

        /* Now check all the predicates. */
        for(Predicate p : query_predicates) {
            /* Check the validity of the attributes. */
            if( (p.lhs() instanceof TupleAttribute) && (!analyzeTupleAttribute((TupleAttribute) p.lhs())) ) 
                return false;
            if( (p.rhs() instanceof TupleAttribute) && (!analyzeTupleAttribute((TupleAttribute) p.rhs())) ) 
                return false;
        }

        /* Finally check the select list. */
        if(select_attributes.size() != 0) {
            for(TupleAttribute ta : select_attributes) {
                /* Check the validity of the attribute. */
                if(!analyzeTupleAttribute(ta))
                    return false;
            }
        } else {
            for(BaseRelationSchema rs : query_relations) {
                for(String attrName : rs.attributeNames) {
                    TupleAttribute ta = new TupleAttribute(rs.getName(), attrName);
                    ta.setRelationSchema(rs);
                    select_attributes.add(ta);
                }
            }
        }

        return true;
    }


    /****************************************************************************************************************
      Query Planning: We use a simple algorithm to create a query plan.

      1. We create a Scan Operator for each relation in the query.

      2. We start with the first join predicate, and make it the lowest join operator in the query plan. Its children 
        are two scan operators.

      3. We find a join predicate such that one of the relations is already incorporated in the query plan, and we
         ada a join operator corresponding to that join predicate. 

      4. We repeat Step 3 until join operators corresponding to all join predicates have been constructed. 

      The code below can only handle well-specified query: for n relation query, there should be exactly n-1 join 
      predicates, and it should be possible to evaluate the query without using Cartesian products. 
      **************************************************************************************************************/

    /* List of operators created. We will split them into Scan operators, and Join operators. */
    Vector<ScanOperator> scan_operators;
    Vector<JoinOperator> join_operators;

    /* We will simply create a scan operator for every relation in the query. */
    void construct_scan_operators() {
        scan_operators = new Vector<ScanOperator>();

        for(BaseRelationSchema rs : query_relations) {
            /* There may be predicates involving just that relation. */
            Vector<Predicate> v = new Vector<Predicate>();

            for(Predicate p : query_predicates) 
                if(p.isScanPredicate(rs))
                    v.add(p);

            scan_operators.add(new ScanOperator(rs, v));
        }
    }

    /* Find the scan operator corresponding to a schema. */
    ScanOperator findScanOperator(BaseRelationSchema rs) {
        for(ScanOperator so : scan_operators) {
            if(so.getRelationSchema() == rs) 
                return so;
        }

        assert false : "Bug: This shouldn't happen";
        return null;
    }

    /* Check if the Scan corresponding to the RelationSchema is already present in the join_operators. */
    boolean relationAlreadyContainedInAJoinOperator(BaseRelationSchema rs) {
        ScanOperator so = findScanOperator(rs);

        for(JoinOperator jo : join_operators) 
            if( (so == jo.getLeftOperator())  || (so == jo.getRightOperator()) )
                return true;
        
        return false;
    }


    /* Create a query plan, the operators etc. */
    boolean plan() {
        construct_scan_operators();

        if(query_relations.size() == 1) {
            /* It is a single table query. We are essentially done. */
            assert scan_operators.size() == 1;
            root = scan_operators.get(0);
        } else {
            /* Before creating the join operators, we will first choose an order in which to do the joins.
               For now, we will use a simple ordering that results in a left-deep plan. 
               We will start with any join predicate, and create a join operator for that using the Scans.
               We will then choose some join predicate that does not require a Cartesian product. */

            /* Let's first create a Vector containing the join predicates. */
            Vector<Predicate> join_predicates_remaining = new Vector<Predicate>();
            for(Predicate p : query_predicates) 
                if(p.isJoinPredicate()) 
                    join_predicates_remaining.add(p);

            if(join_predicates_remaining.size() != (scan_operators.size() - 1)) {
                System.out.println("=========> The query is not well formed");
                return false;
            }

            join_operators = new Vector<JoinOperator>();

            /* We will choose the first join operator arbitrarily. */
            int num_join_predicates = join_predicates_remaining.size();

            for(int i = 0; i < num_join_predicates; i++) {
                if(i == 0) {
                    /* The first join predicate is used to create the bottommost join operator, with two scans as children. */
                    Predicate jp = join_predicates_remaining.get(0);
                    join_predicates_remaining.remove(0);

                    ScanOperator left = findScanOperator(jp.leftRelationSchema());
                    ScanOperator right = findScanOperator(jp.rightRelationSchema());

                    join_operators.add(JoinOperator.createNewJoinOperator(left, right, jp));
                } else {
                    /* Find the first join predicate whose left or right RelationSchema is already present. */
                    Predicate next_jp = null;

                    for(Predicate jp : join_predicates_remaining) {

                        boolean leftContained = relationAlreadyContainedInAJoinOperator(jp.leftRelationSchema());
                        boolean rightContained = relationAlreadyContainedInAJoinOperator(jp.rightRelationSchema());

                        if(leftContained || rightContained) {
                            if(leftContained && rightContained) {
                                System.out.println("=========> Query not well-formed: There is a cycle in the query");
                                return false;
                            }

                            /* We have found a valid join predicate. Break. */
                            next_jp = jp;
                            break;
                        }
                    }

                    if(next_jp == null) {
                        System.out.println("=========> Query not well-formed: I think the query requires a Cartesian Product.");
                        return false;
                    }

                    join_predicates_remaining.removeElement(next_jp);

                    /* One quirk here is that we may need to swap the two arguments of the join predicate to match the left and right. */
                    if(relationAlreadyContainedInAJoinOperator(next_jp.rightRelationSchema())) 
                        next_jp.swap();

                    join_operators.add(JoinOperator.createNewJoinOperator(join_operators.lastElement(), findScanOperator(next_jp.rightRelationSchema()), next_jp));
                }
            }

            /* Finally: we need to set the root to be the very last join operator. */
            root = join_operators.lastElement();
        }

        root = new ProjectOperator(root, select_attributes, distinct, order_by_attributes);

        return true;
    }

    void print() {
        System.out.println("========================================= Query Plan ========================================");
        root.print();
        System.out.println("=============================================================================================");
    }
}
