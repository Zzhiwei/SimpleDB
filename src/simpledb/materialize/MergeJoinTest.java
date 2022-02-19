package simpledb.materialize;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class MergeJoinTest {
	public static void main(String[] args) {
		SimpleDB db = new SimpleDB("studentdb");
		MetadataMgr mdm = db.mdMgr();
		Transaction tx = db.newTx();
		
		Plan studentplan = new TablePlan(tx, "student", mdm);
		Plan enrollplan = new TablePlan(tx, "enroll", mdm);
		
		Plan mergeJoinPlan = new MergeJoinPlan(tx, studentplan, enrollplan, "sid", "studentid");
		
		Scan s = mergeJoinPlan.open();
		
		while (s.next()) {
			System.out.println(s.getString("sid"));
		}
		s.close();
		
		tx.commit();
	}
}
