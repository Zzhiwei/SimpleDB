package simpledb.parse;

import java.util.*;

import simpledb.materialize.AggregationFn;
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
   private List<AggregationFn> aggs;
   private OrderData od;
   
   public QueryData(List<String> fields, Collection<String> tables, Predicate pred, List<String> groupList, List<AggregationFn> aggs, OrderData od) {
      this.fields = fields;
      this.tables = tables;
      this.pred = pred;
      this.groupList= groupList;
      this.aggs = aggs;
      this.od = od;
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
	   return aggs;
   }
   
   public OrderData getOd() {
	   return od;
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
