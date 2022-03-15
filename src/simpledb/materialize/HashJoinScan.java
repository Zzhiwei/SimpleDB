package simpledb.materialize;

import java.util.HashMap;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.RID;


public class HashJoinScan implements Scan {
		
	private String fldname1, fldname2;
	List<TempTable> lhsBuckets;
	List<TempTable> rhsBuckets;
	HashMap<Constant, RID> RidMap = new HashMap<>();
	int index = 0;	
	Scan rhsScan;
	UpdateScan lhsScan;
	
	/**
	 * Create a hash join scan for the given partitioned tables. 
	 * @param fldname1 the LHS join field
	 * @param fldname2 the RHS join field
	 * @param hashtable1 list of temptable (buckets) containing LHS records
	 * @param hashtable2 list of temptable (buckets) containing RHS records
	 */
	HashJoinScan(String fldname1, String fldname2,
			List<TempTable> hashtable1,
			List<TempTable> hashtable2) {		
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.lhsBuckets = hashtable1;
		this.rhsBuckets = hashtable2;				
	}
	
	/**
	 * Positions the bucket index to the first. 
	 */
	public void beforeFirst() {
		index = 0;
	}	
	
	/**
	 * Move to the next record.
	 * At each bucket index, build an in-memory hashtable.
	 * While the RHS bucket has values, read in one by one to probe.
	 * When a match is found, move the LHS scan to the matching record. 
	 */
	public boolean next() {
		while (index < 100) {										
			if (RidMap.isEmpty()) {
				TempTable temp = lhsBuckets.get(index);
				if (temp == null) {
					cleanup();					
					continue;
				}
				UpdateScan sc = temp.open();		
				lhsScan = sc;
				while (sc.next()) {
					RidMap.put(sc.getVal(fldname1), sc.getRid());
				}				
			}
			
			
			if (rhsScan == null) {
				TempTable temp = rhsBuckets.get(index);
				if (temp == null) {
					cleanup();
					continue;
				}
				this.rhsScan = temp.open();
			}
			
			
			while (rhsScan.next()) { 
				Constant c = rhsScan.getVal(fldname2);
				RID rid = RidMap.get(c);
				if (rid != null) {
					lhsScan.moveToRid(rid);
					return true;
				} 
			}
			
			cleanup();
		}
		
		return false;		
	}
	
	/**
	 * Reset data structures before moving to the next bucket.
	 * Increments the bucket index.
	 */
	private void cleanup() {
		index++;
		if (rhsScan != null) {
			rhsScan.close();
		}
		if (lhsScan != null) {			
			lhsScan.close();
		}
		rhsScan = null;				
		lhsScan = null;
		RidMap.clear();
	}
	
	/**
    * Returns the integer value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public int getInt(String fldname) {
      if (rhsScan.hasField(fldname))
         return rhsScan.getInt(fldname);
      else  
         return lhsScan.getInt(fldname);
   }
   
   /**
    * Returns the Constant value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (rhsScan.hasField(fldname))
         return rhsScan.getVal(fldname);
      else
         return lhsScan.getVal(fldname);
   }
   
   /**
    * Returns the string value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public String getString(String fldname) {
      if (rhsScan.hasField(fldname))
         return rhsScan.getString(fldname);
      else
         return lhsScan.getString(fldname);
   }
   
   /** Returns true if the field is in the schema.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
   public boolean hasField(String fldname) {
      return rhsScan.hasField(fldname) || lhsScan.hasField(fldname);
   }
   
   /**
    * Closes the scan by closing LHS scan and RHS scan if they exist.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
	   if (rhsScan != null) {
		   rhsScan.close();   
	   }
	   if (rhsScan != null) {
		   lhsScan.close();   
	   }	   
   }		
}



