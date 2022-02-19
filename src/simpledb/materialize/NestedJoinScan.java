package simpledb.materialize;

import simpledb.index.Index;
import simpledb.record.TableScan;
import simpledb.query.*;

/**
 * The scan class corresponding to the indexjoin relational
 * algebra operator.
 * The code is very similar to that of ProductScan, 
 * which makes sense because an index join is essentially
 * the product of each LHS record with the matching RHS index records.
 * @author Edward Sciore
 */
public class NestedJoinScan implements Scan {
   private Scan lhs;
   private Scan rhs;
   private String fldname1, fldname2;
   
   /**
    * Creates an index join scan for the specified LHS scan and 
    * RHS index.
    * @param lhs the LHS scan
    * @param idx the RHS index
    * @param joinfield the LHS field used for joining
    * @param rhs the RHS scan
    */
   public NestedJoinScan(Scan s1, Scan s2, String fldname1, String fldname2) {
      this.lhs = s1;
      this.rhs = s2;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      beforeFirst();
   }
   
   /**
    * Positions the scan before the first record.
    * That is, the LHS scan will be positioned at its
    * first record, and the index will be positioned
    * before the first record for the join value.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      lhs.beforeFirst();
      rhs.beforeFirst();
      lhs.next();
   }
   
   /**
    * Moves the scan to the next record.
    * The method moves to the next index record, if possible.
    * Otherwise, it moves to the next LHS record and the
    * first index record.
    * If there are no more LHS records, the method returns false.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {  
	   while (true) {
		   Constant v1 = lhs.getVal(fldname1);
		   while (rhs.next()) {
			   Constant v2 = rhs.getVal(fldname2);
				  if (v1.compareTo(v2) == 0) {
					  return true;
				  }
		   }
		   rhs.beforeFirst();
		   
		   if (!lhs.next()) {
			   return false;
		   }
			
	   }
	   
     
          
   }
   
   /**
    * Returns the integer value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public int getInt(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getInt(fldname);
      else  
         return lhs.getInt(fldname);
   }
   
   /**
    * Returns the Constant value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getVal(fldname);
      else
         return lhs.getVal(fldname);
   }
   
   /**
    * Returns the string value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public String getString(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getString(fldname);
      else
         return lhs.getString(fldname);
   }
   
   /** Returns true if the field is in the schema.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
   public boolean hasField(String fldname) {
      return rhs.hasField(fldname) || lhs.hasField(fldname);
   }
   
   /**
    * Closes the scan by closing its LHS scan and its RHS index.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      lhs.close();
      rhs.close();
   }
  
}
