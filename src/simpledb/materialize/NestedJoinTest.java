
package simpledb.materialize;

import simpledb.metadata.MetadataMgr;
import simpledb.plan.Plan;
import simpledb.plan.TablePlan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class NestedJoinTest {
	public static void main(String[] args) {
		// creates new database, metadatamanager and transaction instances
		// for this NestedJoinTest
		SimpleDB db = new SimpleDB("studentdb");
		MetadataMgr mdm = db.mdMgr();
		Transaction tx = db.newTx();

		// create new plan for 'student' and 'enroll' table to nested join during this
		// test
		Plan studentplan = new TablePlan(tx, "student", mdm);
		Plan enrollplan = new TablePlan(tx, "enroll", mdm);

		// create a new NestedJoinPlan to do an equijoin on student.sid and
		// enroll.studentid
		Plan nestedJoinPlan = new NestedJoinPlan(tx, studentplan, enrollplan, "sid", "studentid");

		// open the newly created nestedjoinplan to obtain a scan instance
		Scan s = nestedJoinPlan.open();

		s.beforeFirst();
		// print out all the sname attributes of the records remaining after the join
		while (s.next()) {
			System.out.println(s.getVal("sname"));
		}

		// close the scan after the records have been printed to System.out
		s.close();

		// commit the transaction
		tx.commit();
	}
}
