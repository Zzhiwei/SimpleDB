package simpledb.metadata;

import static java.sql.Types.INTEGER;

import java.util.Map;

import simpledb.tx.Transaction;
import simpledb.record.*;
import simpledb.server.SimpleDB;
import simpledb.index.Index;
import simpledb.index.hash.HashIndex;
import simpledb.parse.BadSyntaxException;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.query.SelectScan;
import simpledb.query.UpdateScan;
import simpledb.index.btree.BTreeIndex; //in case we change to btree indexing


/**
 * The information about an index.
 * This information is used by the query planner in order to
 * estimate the costs of using the index,
 * and to obtain the layout of the index records.
 * Its methods are essentially the same as those of Plan.
 * @author Edward Sciore
 */
public class IndexInfo {
   private String idxname, fldname;
   private Transaction tx;
   private Schema tblSchema;
   private Layout idxLayout;
   private StatInfo si;
   private String indexKeyword;
   
   /**
    * Create an IndexInfo object for the specified index.
    * @param idxname the name of the index
    * @param fldname the name of the indexed field
    * @param tx the calling transaction
    * @param tblSchema the schema of the table
    * @param si the statistics for the table
    */
   public IndexInfo(String idxname, String fldname, Schema tblSchema,
                    Transaction tx,  StatInfo si, String indexKeyword) {
      this.idxname = idxname;
      this.fldname = fldname;
      this.tx = tx;
      this.tblSchema = tblSchema;
      this.idxLayout = createIdxLayout();
      this.si = si;
      this.indexKeyword = indexKeyword;
   }
   
   /**
    * Open the index described by this object.
    * @return the Index object associated with this information
    */
   public Index open() {
	   SimpleDB db = new SimpleDB("studentdb");
	   Transaction tx = db.newTx();
	   MetadataMgr mdm = db.mdMgr();
	   Plan plan = new TablePlan(tx, "student", mdm);
	   UpdateScan scan = (UpdateScan) plan.open();
	   Map<String,IndexInfo> idxinfo = mdm.getIndexInfo("student", tx);
	   
	   
	  if (indexKeyword.equals("hash")) {
		  Index hash = new HashIndex(tx, idxname, idxLayout);
		  for (String fldname : idxinfo.keySet()) {
			  hash.insert(scan.getVal(fldname), scan.getRid());
	      }
		  return hash;
	  } else if (indexKeyword.equals("btree")) {
		  Index btree = new BTreeIndex(tx, idxname, idxLayout);
		  for (String fldname : idxinfo.keySet()) {
			  btree.insert(scan.getVal(fldname), scan.getRid());
	      }
		  return btree;
	  } else {
		  throw new BadSyntaxException();
	  }
   }
   
   public String getIndexKeyword() {
	   return this.indexKeyword;
   }
   
   /**
    * Estimate the number of block accesses required to
    * find all index records having a particular search key.
    * The method uses the table's metadata to estimate the
    * size of the index file and the number of index records
    * per block.
    * It then passes this information to the traversalCost
    * method of the appropriate index type,
    * which provides the estimate.
    * @return the number of block accesses required to traverse the index
    */
   public int blocksAccessed() {
      int rpb = tx.blockSize() / idxLayout.slotSize();
      int numblocks = si.recordsOutput() / rpb;
      if (indexKeyword.equals("hash")) {
    	  return HashIndex.searchCost(numblocks, rpb);    	  
      } else {
    	  return BTreeIndex.searchCost(numblocks, rpb);
      }
   }
   
   /**
    * Return the estimated number of records having a
    * search key.  This value is the same as doing a select
    * query; that is, it is the number of records in the table
    * divided by the number of distinct values of the indexed field.
    * @return the estimated number of records having a search key
    */
   public int recordsOutput() {
      return si.recordsOutput() / si.distinctValues(fldname);
   }
   
   /** 
    * Return the distinct values for a specified field 
    * in the underlying table, or 1 for the indexed field.
    * @param fname the specified field
    */
   public int distinctValues(String fname) {
      return fldname.equals(fname) ? 1 : si.distinctValues(fldname);
   }
   
   /**
    * Return the layout of the index records.
    * The schema consists of the dataRID (which is
    * represented as two integers, the block number and the
    * record ID) and the dataval (which is the indexed field).
    * Schema information about the indexed field is obtained
    * via the table's schema.
    * @return the layout of the index records
    */
   private Layout createIdxLayout() {
      Schema sch = new Schema();
      sch.addIntField("block");
      sch.addIntField("id");
      if (tblSchema.type(fldname) == INTEGER)
         sch.addIntField("dataval");
      else {
         int fldlen = tblSchema.length(fldname);
         sch.addStringField("dataval", fldlen);
      }
      return new Layout(sch);
   }
}