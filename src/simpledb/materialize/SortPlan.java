package simpledb.materialize;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.parse.OrderData;
import simpledb.parse.Pair;
import simpledb.plan.Plan;
import simpledb.query.*;

/**
 * The Plan class for the <i>sort</i> operator.
 * 
 * @author Edward Sciore
 */
public class SortPlan implements Plan {
   private Transaction tx;
   private Plan p;
   private Schema sch;
   private RecordComparator comp;
   private boolean distinct = false;

   /**
    * Create a sort plan for the specified query.
    * 
    * @param p          the plan for the underlying query
    * @param sortfields the fields to sort by
    * @param tx         the calling transaction
    */
   public SortPlan(Transaction tx, Plan p, OrderData sortfields) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      comp = new RecordComparator(sortfields);
   }

   public SortPlan(Transaction tx, Plan p, OrderData sortfields, boolean distinct) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      comp = new RecordComparator(sortfields);
      this.distinct = distinct;
   }

   public SortPlan(Transaction tx, Plan p, List<String> sortfields) {
      this.tx = tx;
      this.p = p;
      sch = p.schema();
      List<Pair> L = new ArrayList<>();
      for (String s : sortfields) {
         L.add(new Pair(s, true));
      }
      comp = new RecordComparator(new OrderData(L));
   }

   /**
    * This method is where most of the action is.
    * Up to 2 sorted temporary tables are created,
    * and are passed into SortScan for final merging.
    * 
    * @see simpledb.plan.Plan#open()
    */
   public Scan open() {
      Scan src = p.open();
      List<TempTable> runs = splitIntoRuns(src);
      src.close();
      while (runs.size() > 1)
         runs = doAMergeIteration(runs);

      TempTable merged = runs.get(0);
      if (distinct) {
         runs.set(0, removeDuplicates(merged));
      }
      return new SortScan(runs, comp);
   }

   /**
    * Return the number of blocks in the sorted table,
    * which is the same as it would be in a
    * materialized table.
    * It does <i>not</i> include the one-time cost
    * of materializing and sorting the records.
    * 
    * @see simpledb.plan.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      // does not include the one-time cost of sorting
      Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
      return mp.blocksAccessed();
   }

   /**
    * Return the number of records in the sorted table,
    * which is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p.recordsOutput();
   }

   /**
    * Return the number of distinct field values in
    * the sorted table, which is the same as in
    * the underlying query.
    * 
    * @see simpledb.plan.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      return p.distinctValues(fldname);
   }

   /**
    * Return the schema of the sorted table, which
    * is the same as in the underlying query.
    * 
    * @see simpledb.plan.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }

   /**
    * As per SortMergeJoin procedure, split the input Scan
    * into multiple runs and store them as TempTables in a list
    * 
    * @param src - the specified scan to be split into sorted runs
    * @return - the sorted runs in an ArrayList of TempTables
    */
   private List<TempTable> splitIntoRuns(Scan src) {
      List<TempTable> temps = new ArrayList<>();
      src.beforeFirst();
      if (!src.next())
         return temps;
      TempTable currenttemp = new TempTable(tx, sch);
      temps.add(currenttemp);
      UpdateScan currentscan = currenttemp.open();
      while (copy(src, currentscan))
         if (comp.compare(src, currentscan) < 0) {
            // start a new run
            currentscan.close();
            currenttemp = new TempTable(tx, sch);
            temps.add(currenttemp);
            currentscan = (UpdateScan) currenttemp.open();
         }
      currentscan.close();
      return temps;
   }

   /**
    * Second phase of SortMergeJoin where the sorted runs are merged.
    * Iterates and merges the first two runs in the input ArrayList
    * until only 1 sorted run remains in the list.
    * 
    * @param runs - the list of runs to merge
    * @return - an ArrayList of size 1 with the merged run as the first element
    */
   private List<TempTable> doAMergeIteration(List<TempTable> runs) {
      List<TempTable> result = new ArrayList<>();
      while (runs.size() > 1) {
         TempTable p1 = runs.remove(0);
         TempTable p2 = runs.remove(0);
         result.add(mergeTwoRuns(p1, p2));
      }
      if (runs.size() == 1)
         result.add(runs.get(0));
      return result;
   }

   /**
    * Helper function to merge two runs done by comparing the first value of p1 and
    * p2
    * and adding the smaller value until all values of both tables have been
    * copied.
    * 
    * @param p1 - the first table to be merged
    * @param p2 - the second table to be merged
    * @return - resultant TempTable after the merging of both runs
    */
   private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
      Scan src1 = p1.open();
      Scan src2 = p2.open();
      TempTable result = new TempTable(tx, sch);
      UpdateScan dest = result.open();

      boolean hasmore1 = src1.next();
      boolean hasmore2 = src2.next();
      while (hasmore1 && hasmore2)
         if (comp.compare(src1, src2) < 0)
            hasmore1 = copy(src1, dest);
         else
            hasmore2 = copy(src2, dest);

      if (hasmore1)
         while (hasmore1)
            hasmore1 = copy(src1, dest);
      else
         while (hasmore2)
            hasmore2 = copy(src2, dest);
      src1.close();
      src2.close();
      dest.close();
      return result;
   }

   /**
    * Copies the current value pointed to in src to the pointer in dest
    * 
    * @param src  - the scan to be copied from
    * @param dest - the scan to be copied to
    * @return - true if and only if there if src.next() returns true
    */
   private boolean copy(Scan src, UpdateScan dest) {
      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, src.getVal(fldname));
      return src.next();
   }

   /**
    * Helper function to removeDuplicates for a DISTINCT query
    * Opens two scans of the input TempTable and iterates through both, only
    * copying
    * non-duplicate values to the resultant table.
    * 
    * @param merged - the TempTable with duplicates to be removed
    * @return - TempTable without duplicates
    */
   private TempTable removeDuplicates(TempTable merged) {
      TempTable res = new TempTable(tx, sch);
      Scan sc1 = merged.open();
      Scan sc2 = merged.open();
      UpdateScan dest = res.open();

      sc1.beforeFirst();
      sc2.beforeFirst();
      sc2.next();

      while (sc2.next()) {
         sc1.next();
         boolean isDuplicate = true;
         for (String fldname : sch.fields()) {
            Constant sc1Value = sc1.getVal(fldname);
            Constant sc2Value = sc2.getVal(fldname);
            int comparison = sc1Value.compareTo(sc2Value);
            if (comparison != 0) {
               isDuplicate = false;
            }
         }
         if (!isDuplicate) {
            dest.insert();
            for (String fldname : sch.fields())
               dest.setVal(fldname, sc1.getVal(fldname));
         }
      }

      dest.insert();
      for (String fldname : sch.fields())
         dest.setVal(fldname, sc1.getVal(fldname));

      sc1.close();
      sc2.close();
      dest.close();
      return res;
   }
}
