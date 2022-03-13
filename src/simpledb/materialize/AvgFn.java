package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>count</i> aggregation function.
 * @author Edward Sciore
 */
public class AvgFn implements AggregationFn {
   private String fldname;
   private int sum;
   private int num;
   
   /**
    * Create a sum aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname) {
      this.fldname = fldname;
      this.sum = 0;
      this.num = 0;
   }
  
   public void processFirst(Scan s) {
	   sum = s.getInt(fldname);
	   num = 1;
   }
   
   public void processNext(Scan s) {
      sum += s.getInt(fldname);
      num++;
   }
   
   public String fieldName() {
      return "avgof" + fldname;
   }
   
   public Constant value() {
      return new Constant(Math.round(sum/num));
   }
}
