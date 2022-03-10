package simpledb.materialize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import simpledb.parse.OrderData;
import simpledb.parse.Pair;
import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {
	   private Plan p1, p2;
	   private String fldname1, fldname2;
	   private Schema sch = new Schema();
	   private Transaction tx;
	   
	   /**
	    * Creates a mergejoin plan for the two specified queries.
	    * The RHS must be materialized after it is sorted, 
	    * in order to deal with possible duplicates.
	    * @param p1 the LHS query plan
	    * @param p2 the RHS query plan
	    * @param fldname1 the LHS join field
	    * @param fldname2 the RHS join field
	    * @param tx the calling transaction
	    */
	   public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
		   this.fldname1 = fldname1;
		   this.fldname2 = fldname2;
		   this.p1 = p1;
		   this.p2 = p2;      
		   sch.addAll(p1.schema());
		   sch.addAll(p2.schema());
		   this.tx = tx;
		   
		  
		   
		   					 
	   }
	   
	   /** The method first sorts its two underlying scans
	     * on their join field. It then returns a mergejoin scan
	     * of the two sorted table scans.
	     * @see simpledb.plan.Plan#open()
	     */
	   public Scan open() {
		   //asuming 100 buffers for partitioning 		   
		   List<UpdateScan> lhsBuckets = new ArrayList<>(100);
		   List<UpdateScan> rhsBuckets = new ArrayList<>(100);
		   Scan s1 = p1.open();
		   Scan s2 = p2.open();
		   
		   while (s1.next()) {
			   Schema p1Schema = p1.schema();
			   Constant s1Val = s1.getVal(fldname1);
			   int bucketIndex = s1Val.hashCode() % 100;
			   UpdateScan sc = lhsBuckets.get(bucketIndex);
			   if (sc == null) {
				   TempTable temp = new TempTable(tx, p1Schema);	      
				   sc = temp.open();
				   sc.insert();
				   for (String fldname : p1Schema.fields()) {
					   sc.setVal(fldname, s1.getVal(fldname));
				   }				   
			   } else {
				   sc = lhsBuckets.get(bucketIndex);
				   sc.insert();
				   for (String fldname : p1Schema.fields()) {
					   sc.setVal(fldname, s1.getVal(fldname));
				   }
			   }
		   		   
		   }

		   while (s2.next()) {
			   Schema p2Schema = p2.schema();
			   Constant s1Val = s1.getVal(fldname1);
			   int bucketIndex = s1Val.hashCode() % 100;
			   UpdateScan sc = lhsBuckets.get(bucketIndex);
			   if (sc == null) {
				   TempTable temp = new TempTable(tx, p1Schema);	      
				   sc = temp.open();
				   sc.insert();
				   for (String fldname : p1Schema.fields()) {
					   sc.setVal(fldname, s1.getVal(fldname));
				   }				   
			   } else {
				   sc = lhsBuckets.get(bucketIndex);
				   sc.insert();
				   for (String fldname : p1Schema.fields()) {
					   sc.setVal(fldname, s1.getVal(fldname));
				   }
			   }
		   		   
		   }
		 
		  	      
		   
	      return (Scan) new HashJoinScan(s1, s2, fldname1, fldname2, lhsHashtable, rhsHashtable);
	   }
	   
	   /**
	    * Return the number of block acceses required to
	    * mergejoin the sorted tables.
	    * Since a mergejoin can be preformed with a single
	    * pass through each table, the method returns
	    * the sum of the block accesses of the 
	    * materialized sorted tables.
	    * It does <i>not</i> include the one-time cost
	    * of materializing and sorting the records.
	    * @see simpledb.plan.Plan#blocksAccessed()
	    */
	   public int blocksAccessed() {
	      return p1.blocksAccessed() + p2.blocksAccessed();
	   }
	   
	   /**
	    * Return the number of records in the join.
	    * Assuming uniform distribution, the formula is:
	    * <pre> R(join(p1,p2)) = R(p1)*R(p2)/max{V(p1,F1),V(p2,F2)}</pre>
	    * @see simpledb.plan.Plan#recordsOutput()
	    */
	   public int recordsOutput() {
	      int maxvals = Math.max(p1.distinctValues(fldname1),
	                             p2.distinctValues(fldname2));
	      return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
	   }
	   
	   /**
	    * Estimate the distinct number of field values in the join.
	    * Since the join does not increase or decrease field values,
	    * the estimate is the same as in the appropriate underlying query.
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
	    * @see simpledb.plan.Plan#schema()
	    */
	   public Schema schema() {
	      return sch;
	   }
	}