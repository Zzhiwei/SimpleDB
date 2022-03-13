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
		   List<TempTable> lhsBuckets = new ArrayList<>(100);		   		   
		   List<TempTable> rhsBuckets = new ArrayList<>(100);
		   Schema p1Schema = p1.schema();
		   Schema p2Schema = p2.schema();		   
		   Scan s1 = p1.open();
		   Scan s2 = p2.open();
		   UpdateScan sc;
		   for (int i = 0; i < 200; i++) {
			   lhsBuckets.add(null);
			   rhsBuckets.add(null);
		   }
		   
		   while (s1.next()) {			   
			   Constant s1Val = s1.getVal(fldname1);
			   int bucketIndex = s1Val.hashCode() % 100;
			   TempTable bucket = lhsBuckets.get(bucketIndex);	
			   System.out.println("inserting record into bucket" + bucketIndex);
			   if (bucket == null) {
				   //create new bucket if no bucket yet
				   bucket = new TempTable(tx, p1Schema);	     
				   lhsBuckets.set(bucketIndex, bucket);
				   sc = bucket.open();
				   sc.insert();
				   for (String fldname : p1Schema.fields()) {
					   System.out.println("inserting " + s1.getVal(fldname) + " to " + fldname);
					   sc.setVal(fldname, s1.getVal(fldname));
				   }				   
			   } else {
				   // open bucket scan and insert values
				   sc = bucket.open();
				   sc.insert();
				   for (String fldname : p1Schema.fields()) {
					   sc.setVal(fldname, s1.getVal(fldname));
				   }
			   }
			   sc.close();
			   
			      		   		   
		   }
		   
		   while (s2.next()) {			   
			   Constant s2Val = s2.getVal(fldname2);
			   int bucketIndex = s2Val.hashCode() % 100;
			   TempTable bucket = rhsBuckets.get(bucketIndex);	
//			   System.out.println("inserting record into bucket" + bucketIndex);
			   if (bucket == null) {
				   //create new bucket if no bucket yet
				   bucket = new TempTable(tx, p2Schema);	     
				   rhsBuckets.set(bucketIndex, bucket);
				   sc = bucket.open();
				   sc.insert();
				   for (String fldname : p2Schema.fields()) {
//					   System.out.println("inserting " + s1.getVal(fldname) + " to " + fldname);
					   sc.setVal(fldname, s2.getVal(fldname));
				   }				   
			   } else {
				   // open bucket scan and insert values
				   sc = bucket.open();
				   sc.insert();
				   for (String fldname : p2Schema.fields()) {
					   sc.setVal(fldname, s2.getVal(fldname));
				   }
			   }
			   sc.close();
			   
			      		   		   
		   }
		  
		   
	      return (Scan) new HashJoinScan(fldname1, fldname2, lhsBuckets, rhsBuckets);
		   
		   
//		   for (TempTable bucket: lhsBuckets) {
//		   if (bucket == null) {
//			   continue;
//		   }
//		   System.out.println("trying to print out this bucket");
//		   
//		   sc = bucket.open();
//		   sc.beforeFirst();
//		   while (sc.next()) {				   
//			   for (String fldname : p1Schema.fields()) {
//				   System.out.print(sc.getVal(fldname));
//				   System.out.print(" | ");
//			   }
//			   System.out.println();
//		   }
//		   sc.close();
//	   }
	   
	   

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