package simpledb.materialize;

import simpledb.query.*;

/**
 * The scan class corresponding to the nestedjoin relational
 * algebra operator.
 * 
 * @author Edward Sciore
 */
public class NestedJoinScan implements Scan {
   private Scan lhs;
   private Scan rhs;
   private String fldname1, fldname2;

   /**
    * Creates a nestedjoin join scan for the specified LHS scan and
    * RHS index.
    * 
    * @param lhs      the LHS scan
    * @param rhs      the RHS scan
    * @param fldname1 the fieldname in lhs to equijoin on
    * @param fldname2 the fieldname in rhs to equijoin on
    */
   public NestedJoinScan(Scan lhs, Scan rhs, String fldname1, String fldname2) {
      this.lhs = lhs;
      this.rhs = rhs;
      this.fldname1 = fldname1;
      this.fldname2 = fldname2;
      beforeFirst();
   }

   /**
    * Positions the scan before the first record.
    * That is, the LHS scan will be positioned at its
    * first record, and the index will be positioned
    * before the first record for the join value.
    * 
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      lhs.beforeFirst();
      rhs.beforeFirst();
      lhs.next();
   }

   /**
    * Moves the scan to the next record.
    * The method iterates through the rhs until the equijoin
    * condition is met, if possible.
    * Otherwise, it moves to the next LHS record and iterates through
    * the rhs to find the next record that matches the equijoin condition.
    * If there are no more LHS records, the method returns false.
    * 
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
    * 
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
    * 
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
    * 
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public String getString(String fldname) {
      if (rhs.hasField(fldname))
         return rhs.getString(fldname);
      else
         return lhs.getString(fldname);
   }

   /**
    * Returns true if the field is in the schema.
    * 
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return rhs.hasField(fldname) || lhs.hasField(fldname);
   }

   /**
    * Closes the scan by closing both the lhs and rhs scans.
    * 
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      lhs.close();
      rhs.close();
   }

}
