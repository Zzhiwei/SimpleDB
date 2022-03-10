package simpledb.materialize;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;

public class HashJoinScan {
	//next() 
	//read in lhs[0], insert all the records in this bucket into a hashtable
	//for rhs, read in one record at a time, hash it, and see if it matches with any rec 
	//in the hastable
	
	private Scan lhs;
	private Scan rhs;
	private String fldname1, fldname2;
	List<List<Constant>> lhsBuckets;
	List<List<Constant>> rhsBuckets;
	HashMap<Constant> currHashTable;
	
	
	

	HashJoinScan(Scan s1, Scan s2, String fldname1, String fldname2,
			List<List<Constant>> hashtable1,
			List<List<Constant>> hashtable2) {
		this.lhs = s1;
		this.rhs = s2;
		this.fldname1 = fldname1;
		this.fldname2 = fldname2;
		this.lhsBuckets = hashtable1;
		this.rhsBuckets = hashtable2;				
	}
	
	
	public boolean next() {
		
		
	}
	

	
	

}



