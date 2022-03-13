package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.HashSet;


import simpledb.query.Constant;
import simpledb.query.Scan;


public class HashJoinScan implements Scan {
		
	private String fldname1, fldname2;
	List<TempTable> lhsBuckets;
	List<TempTable> rhsBuckets;
	int index = 0;	
	HashSet<Constant> set = new HashSet<>();
	Scan rhsScan;
	Scan lhsScan;
	
	
	
	

	HashJoinScan(String fldname1, String fldname2,
			List<TempTable> hashtable1,
			List<TempTable> hashtable2) {		
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.lhsBuckets = hashtable1;
		this.rhsBuckets = hashtable2;				
	}
	
	public void beforeFirst() {
//	      lhs.beforeFirst();
//	      rhs.beforeFirst();
//	      lhs.next();
	   }
	
	
	public boolean next() {
		//<Constant, record>
		while (index < 100) {										
			if (set.isEmpty()) { //build hashtable only if set is empty
				TempTable temp = lhsBuckets.get(index);
				if (temp == null) {
					index++;					
					continue;
				}
				Scan sc = temp.open();				
				while (sc.next()) {
					set.add(sc.getVal(fldname1));					
				}
				sc.close();
			}
			
			
			if (rhsScan == null) {
				TempTable temp = rhsBuckets.get(index);
				if (temp == null) {
					index++;
					set.clear();
					continue;
				}
				this.rhsScan = temp.open();
			}
			
			
			while (rhsScan.next()) { //move the pointer forward 
				//check whether hashset contains that value
				Constant c = rhsScan.getVal(fldname2);
				if (set.contains(c)) {					
					return true;
				} 
			}
			
			index++;
			set.clear();
			rhsScan = null;
		}
		
		return false;
		
	}
	
	/**
	    * Returns the integer value of the specified field.
	    * @see simpledb.query.Scan#getVal(java.lang.String)
	    */
	   public int getInt(String fldname) {
	      if (rhsScan.hasField(fldname))
	         return rhsScan.getInt(fldname);
	      else  
	         return rhsScan.getInt(fldname);
	   }
	   
	   /**
	    * Returns the Constant value of the specified field.
	    * @see simpledb.query.Scan#getVal(java.lang.String)
	    */
	   public Constant getVal(String fldname) {
	      if (rhsScan.hasField(fldname))
	         return rhsScan.getVal(fldname);
	      else
	         return rhsScan.getVal(fldname);
	   }
	   
	   /**
	    * Returns the string value of the specified field.
	    * @see simpledb.query.Scan#getVal(java.lang.String)
	    */
	   public String getString(String fldname) {
	      if (rhsScan.hasField(fldname))
	         return rhsScan.getString(fldname);
	      else
	         return rhsScan.getString(fldname);
	   }
	   
	   /** Returns true if the field is in the schema.
	     * @see simpledb.query.Scan#hasField(java.lang.String)
	     */
	   public boolean hasField(String fldname) {
	      return rhsScan.hasField(fldname) || rhsScan.hasField(fldname);
	   }
	   
	   /**
	    * Closes the scan by closing its LHS scan and its RHS index.
	    * @see simpledb.query.Scan#close()
	    */
	   public void close() {
		   rhsScan.close();
		   rhsScan.close();
	   }

	
	

}



