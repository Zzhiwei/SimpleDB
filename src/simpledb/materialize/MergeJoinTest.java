package simpledb.materialize;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class MergeJoinTest {
	public static void main(String[] args) {

		// creates new database, metadatamanager and transaction instances for the
		// MergeJoinTest
		SimpleDB db = new SimpleDB("studentdb");
		MetadataMgr mdm = db.mdMgr();
		Transaction tx = db.newTx();

		// create new plan for 'student' and 'enroll' table to merge during this test
		Plan studentplan = new TablePlan(tx, "student", mdm);
		Plan enrollplan = new TablePlan(tx, "enroll", mdm);

		// create a new MergeJoinPlan to do an equijoin on student.sid and
		// enroll.studentid
		Plan mergeJoinPlan = new MergeJoinPlan(tx, studentplan, enrollplan, "sid", "studentid");

		// open the plan to obtain a scan instance
		Scan s = mergeJoinPlan.open();

		// print out all the sname attributes of the records remaining after the merge
		// join
		while (s.next()) {
			System.out.println(s.getString("sname"));
		}

		// close the scan after the records have been printed to System.out
		s.close();

		// commit the transaction
		tx.commit();
	}
}
