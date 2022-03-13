package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>count</i> aggregation function.
 * @author Edward Sciore
 */
public class SumFn implements AggregationFn {
   private String fldname;
   private int sum;
   
   /**
    * Create a sum aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public SumFn(String fldname) {
      this.fldname = fldname;
      this.sum = 0;
   }
  
   public void processFirst(Scan s) {
	   sum = s.getInt(fldname);
   }
   
   public void processNext(Scan s) {
      sum += s.getInt(fldname);
   }
   
   public String fieldName() {
      return "sumof" + fldname;
   }
   
   public Constant value() {
      return new Constant(sum);
   }
}
