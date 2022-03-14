import java.util.Scanner;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class SimpleIJ {
   public static void main(String[] args) {
	  
		 Scanner sc = new Scanner(System.in);
		 SimpleDB db = new SimpleDB("studentdb");
		
		 Transaction tx  = db.newTx();
		 Planner planner = db.planner();

         System.out.print("\nSQL> ");
         while (sc.hasNextLine()) {
            // process one line of input
            String cmd = sc.nextLine().trim();
            if (cmd.startsWith("exit"))
               break;
            else if (cmd.startsWith("select"))
               doQuery(planner, tx, cmd);
            else
               doUpdate(planner, tx, cmd);
            System.out.print("\nSQL> ");
         }
         sc.close();
      
      
   }

   private static void doQuery(Planner planner, Transaction tx, String cmd) {
      
         Plan p = planner.createQueryPlan(cmd, tx);
         Scan s = p.open();
    	 
         int distinctIndex = cmd.indexOf("distinct");
         int endIndex = cmd.indexOf("from");
         String cols;
         if (distinctIndex == -1) {
        	 cols = cmd.substring(7, endIndex);        	 
         } else {
        	 cols = cmd.substring(distinctIndex + 8, endIndex);
         }
         String[] headers = cols.split(",");
         int numcols = headers.length;
         int totalwidth = 0;

         // print header
         for(int i=0; i<numcols; i++) {
            String fldname = headers[i].trim();
            int width = 20 ; // not sure about this, hard coding it as 30 first
            totalwidth += width;
            String fmt = "%" + width + "s";
            System.out.format(fmt, fldname);
         }
         System.out.println();
         for(int i=0; i<totalwidth; i++)
            System.out.print("-");
         System.out.println();

         // print records
         while(s.next()) {
            for (int i=0; i<numcols; i++) {
               String fldname = headers[i].trim();
               int fldtype = fldname.equalsIgnoreCase("sname") || fldname.equalsIgnoreCase("dname") || fldname.equalsIgnoreCase("title") || fldname.equalsIgnoreCase("prof") || fldname.equalsIgnoreCase("grade") ? 0 : 1;
               String fmt = "%" + 20; // not sure about this, hard coding it as 30 first
               if (fldtype == 1) {
                  int ival = s.getInt(fldname);
                  System.out.format(fmt + "d", ival);
               }
               else {
                  String sval = s.getString(fldname);
                  System.out.format(fmt + "s", sval);
               }
            }
            System.out.println();
         }
      
   }

   private static void doUpdate(Planner planner, Transaction tx, String cmd) {
      
         int howmany = planner.executeUpdate(cmd, tx);
         System.out.println(howmany + " records processed");
         tx.commit();
      
   }
}