package simpledb.materialize;

import simpledb.tx.Transaction;
import simpledb.plan.Plan;
import simpledb.query.*;
import simpledb.record.*;

/**
 * The Plan class for the <i>nestedjoin</i> operator.
 * 
 * @author Edward Sciore
 */
public class NestedJoinPlan implements Plan {
   private Plan p1, p2;
   private String fldname1, fldname2;
   private Schema sch = new Schema();

   /**
    * Creates a nestedjoin plan for the two specified queries.
    * The RHS must be materialized after it is sorted,
    * in order to deal with possible duplicates.
    * 
    * @param p1       the LHS query plan
    * @param p2       the RHS query plan
    * @param fldname1 the LHS join field
    * @param fldname2 the RHS join field
    * @param tx       the calling transaction
    */
   public NestedJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      this.p1 = p1;
      this.p2 = p2;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }

   /**
    * The method first opens a scan for Plan p1 and p2 respectively.
    * It then returns a NestedJoinScan to join s1 and s2 on
    * s1.fldname1 = s2.fldname2
    * 
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan s1 = p1.open();
      Scan s2 = p2.open();
      return new NestedJoinScan(s1, s2, fldname1, fldname2);
   }

   /**
    * Return the number of block accesses required to
    * nestedjoin the sorted tables.
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p1.blocksAccessed() + p2.blocksAccessed();
   }

   /**
    * Return the number of records in the join.
    * Assuming uniform distribution, the formula is:
    * 
    * <pre>
    *  R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}
    * </pre>
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      // int maxvals = Math.max(p1.distinctValues(fldname1),
      // p2.distinctValues(fldname2));
      // return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
      return 3;
   }

   /**
    * Estimate the distinct number of field values in the join.
    * Since the join does not increase or decrease field values,
    * the estimate is the same as in the appropriate underlying query.
    * 
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }

   /**
    * Return the schema of the join,
    * which is the union of the schemas of the underlying queries.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }
}
