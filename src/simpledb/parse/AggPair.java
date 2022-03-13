package simpledb.parse;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.AvgFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.materialize.MinFn;
import simpledb.materialize.SumFn;

public class AggPair {
	private String fldname;
	private AggregationFn agg;
	
	public AggPair(String fldname, AggregationFn agg) {
		this.fldname = fldname;
		this.agg = agg;
	}
	
	public String getFldname() {
		return fldname;
	}
	
	public AggregationFn getAgg() {
		return agg;
	}
}
