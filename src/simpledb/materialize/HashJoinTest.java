package simpledb.materialize;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class HashJoinTest {
	public static void main(String[] args) {
	SimpleDB db = new SimpleDB("studentdb");
	MetadataMgr mdm = db.mdMgr();
	Transaction tx = db.newTx();
	
	Plan studentplan = new TablePlan(tx, "student", mdm);
	Plan enrollplan = new TablePlan(tx, "enroll", mdm);
	
	Plan hashJoinPlan = new HashJoinPlan(tx, studentplan, enrollplan, "sid", "studentid");
	hashJoinPlan.open();
	}
}
