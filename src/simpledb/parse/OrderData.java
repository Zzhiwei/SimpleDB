package simpledb.parse;

import java.util.*;

/**
 * Data for the SQL <i>order by</i> statement.
 * @author Edward Sciore
 */
public class OrderData {
   private List<Pair> L;
   
   /**
    * Creates order data object to be used for query.
    * @param L List of field name and isAscending boolean pairs 
    */
   public OrderData(List<Pair> L) {
      this.L = L;
   }
   
   /**
    * Returns List of field name and isAscending boolean pairs
    * @return List of field name and isAscending boolean pairs
    */
   public List<Pair> getPairs() {
      return L;
   }
}
