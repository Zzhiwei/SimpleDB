package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.AvgFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.materialize.MinFn;
import simpledb.materialize.SumFn;
import simpledb.query.*;

/**
 * Data for the SQL <i>select</i> statement.
 * @author Edward Sciore
 */
public class QueryData {
   private List<String> fields;
   private Collection<String> tables;
   private Predicate pred;
   private List<String> groupList;
   private List<AggregationFn> aggs = new ArrayList<>();
   private OrderData od;
   private boolean distinct;
   
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String> groupList, OrderData od, boolean distinct) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.groupList= groupList;
      this.od = od;
      this.distinct = distinct;
   }
   
   /**
    * Returns the fields mentioned in the select clause.
    * @return a list of field names
    */
   public List<String> fields() {
      return fields;
   }
   
   /**
    * Returns the tables mentioned in the from clause.
    * @return a collection of table names
    */
   public Collection<String> tables() {
      return tables;
   }
   
   public List<String> getGroupList() {
	   return groupList;
   }
   
   public List<AggregationFn> getAggs() {
	  List<AggregationFn> aggLst = new ArrayList<>();
	  for (String s : fields) {
		  if (s.startsWith("sumof")) {
	    	  aggLst.add(new SumFn(s.substring(5)));
	      } else if (s.startsWith("maxof")) {
	    	  aggLst.add(new MaxFn(s.substring(5)));
	      } else if (s.startsWith("minof")) {
	    	  aggLst.add(new MinFn(s.substring(5)));
	      } else if (s.startsWith("avgof")) {
	    	  aggLst.add(new AvgFn(s.substring(5)));
	      } else if (s.startsWith("countof")) {
	    	  aggLst.add(new CountFn(s.substring(7)));
	      }
	  }
	  return aggLst;
   }
   
   public OrderData getOd() {
	   return od;
   }
   
   public boolean getDistinct() {
	   return distinct;
   }
   
   
   /**
    * Returns the predicate that describes which
    * records should be in the output table.
    * @return the query predicate
    */
   public Predicate pred() {
      return pred;
   }
   
   public String toString() {
      String result = "select ";
      for (String fldname : fields)
         result += fldname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      result += " from ";
      for (String tblname : tables)
         result += tblname + ", ";
      result = result.substring(0, result.length()-2); //remove final comma
      String predstring = pred.toString();
      if (!predstring.equals(""))
         result += " where " + predstring;
      return result;
   }
}
