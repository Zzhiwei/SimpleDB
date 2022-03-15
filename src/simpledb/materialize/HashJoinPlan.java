package simpledb.materialize;

import java.util.ArrayList;
import java.util.List;

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
	    * Creates a merge join plan for the specified queries.
	    * Both the LHS and RHS are partitioned and materialized.
	    * @param tx the calling transaction
	    * @param p1 the LHS query plan
	    * @param p2 the RHS query plan
	    * @param fldname1 the LHS join field
	    * @param fldname2 the RHS join field
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
	   	   	   
	   /**
	    * This method first partitions the input plans into
	    * buckets and materializes the buckets.
	    * A hash join scan is then returned which would
	    * carry out the search phase using partitioned buckets  
	    */
	   public Scan open() {
		   //assuming 100 buffers for partitioning 		   
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
		   
		   //hashes all the values from LHS table into their respective buckets 
		   while (s1.next()) {			   
			   Constant s1Val = s1.getVal(fldname1);
			   int bucketIndex = s1Val.hashCode() % 100;
			   TempTable bucket = lhsBuckets.get(bucketIndex);	
			   System.out.println("inserting record(" + fldname1 + ": " + s1Val + ") into bucket " + bucketIndex);
			   if (bucket == null) {
				   //create new bucket if no bucket yet
				   bucket = new TempTable(tx, p1Schema);	     
				   lhsBuckets.set(bucketIndex, bucket);
				   sc = bucket.open();
				   sc.insert();
				   for (String fldname : p1Schema.fields()) {					   
					   sc.setVal(fldname, s1.getVal(fldname));
				   }				   
			   } else {
				   // open bucket update scan and insert values
				   sc = bucket.open();
				   sc.insert();
				   for (String fldname : p1Schema.fields()) {
					   sc.setVal(fldname, s1.getVal(fldname));
				   }
			   }
			   sc.close();
			   
			      		   		   
		   }
		   
		   //hashes all the values from RHS table into their respective buckets
		   while (s2.next()) {			   
			   Constant s2Val = s2.getVal(fldname2);
			   int bucketIndex = s2Val.hashCode() % 100;
			   TempTable bucket = rhsBuckets.get(bucketIndex);	
			   System.out.println("inserting record(" + fldname2 + ": " + s2Val + ") into bucket " + bucketIndex);
			   if (bucket == null) {
				   //create new bucket if no bucket yet
				   bucket = new TempTable(tx, p2Schema);	     
				   rhsBuckets.set(bucketIndex, bucket);
				   sc = bucket.open();
				   sc.insert();
				   for (String fldname : p2Schema.fields()) {					   
					   sc.setVal(fldname, s2.getVal(fldname));
				   }				   
			   } else {
				   // open bucket update scan and insert values
				   sc = bucket.open();
				   sc.insert();
				   for (String fldname : p2Schema.fields()) {
					   sc.setVal(fldname, s2.getVal(fldname));
				   }
			   }
			   sc.close();			   			      		   		   
		   }		  
		   
	      return (Scan) new HashJoinScan(fldname1, fldname2, lhsBuckets, rhsBuckets);
	   }
	   
	   /**
	    * Return the number of block accesses required to
	    * hash join the tables.
	    * Since a hash join can be performed with one pass
	    * and one read, the method returns the sum
	    * of the block accesses of the plans times 2.
	    */
	   public int blocksAccessed() {
	      return 2 * (p1.blocksAccessed() + p2.blocksAccessed());
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