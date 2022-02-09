package simpledb.parse;

import java.util.*;

/**
 * Data for the SQL <i>order by</i> statement.
 * @author Edward Sciore
 */
public class OrderData {
   private List<Pair> L;
   
   public OrderData(List<Pair> L) {
      this.L = L;
   }
   
   public List<Pair> getPairs() {
      return L;
   }
}
