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
    * Create an average aggregation function for the specified field.
    * @param fldname the name of the aggregated field
    */
   public AvgFn(String fldname) {
      this.fldname = fldname;
      this.sum = 0;
      this.num = 0;
   }
  
   /**
    * Start calculating a new average.
    * Since SimpleDB does not support null values,
    * every record will be counted,
    * regardless of the field.
    * The current number of records is thus set to 1.
    * The initial sum is also set to value in the current record.
    * @see simpledb.materialize.AggregationFn#processFirst(simpledb.query.Scan)
    */
   public void processFirst(Scan s) {
	   sum = s.getInt(fldname);
	   num = 1;
   }
   
   /**
    * Since SimpleDB does not support null values,
    * this method always increments the sum and number of records,
    * regardless of the field.
    * @see simpledb.materialize.AggregationFn#processNext(simpledb.query.Scan)
    */
   public void processNext(Scan s) {
      sum += s.getInt(fldname);
      num++;
   }
   
   /**
    * Return the field's name, prepended by "avgof".
    * @see simpledb.materialize.AggregationFn#fieldName()
    */
   public String fieldName() {
      return "avgof" + fldname;
   }
   
   /**
    * Return the current average.
    * @see simpledb.materialize.AggregationFn#value()
    */
   public Constant value() {
      return new Constant(Math.round(sum/num));
   }
}
