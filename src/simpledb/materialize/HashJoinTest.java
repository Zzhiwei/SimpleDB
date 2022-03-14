package simpledb.materialize;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
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
		Scan s = hashJoinPlan.open();
				
		System.out.println("printing result");
		while (s.next()) {			
			System.out.println(s.getVal("sname"));
		}
		s.close();
		
		Plan courseplan = new TablePlan(tx, "course", mdm);
		Plan sectionplan = new TablePlan(tx, "section", mdm);
		hashJoinPlan = new HashJoinPlan(tx, courseplan, sectionplan, "cid", "courseid");
		s = hashJoinPlan.open();
			
		System.out.println("printing join result:");
		while (s.next()) {			
			System.out.println(s.getVal("cid"));
		}
		s.close();
		
		tx.commit();
	}
}
