package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;
   
   public Parser(String s) {
      lex = new Lexer(s);
   }
   
// Methods for parsing predicates, terms, expressions, constants, and fields
   
   public String field() {
      return lex.eatId();
   }
   
   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }
   
   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }
   
   public Term term() {
      Expression lhs = expression();
      Operator operator = lex.eatOperator();
      Expression rhs = expression();
      return new Term(lhs, rhs, operator);
   }
   
   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }
   
// Methods for parsing queries
   
   public QueryData query() {
      lex.eatKeyword("select");
      // System.out.println("Ate select");
      
      List<String> fields = selectList();
      lex.eatKeyword("from");
      // System.out.println("Ate from");
      
      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      OrderData od;
      if (lex.matchKeyword("order")) {
    	  od = order();
      } else {
    	  List<Pair> L = new ArrayList<>();
    	  for (String s : fields) {
    		  L.add(new Pair(s, true));
    	  }
    	  od = new OrderData(L);
      }
      return new QueryData(fields, tables, pred, od);    	  
   }
   
   private List<String> selectList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      }
      return L;
   }
   
   private List<Pair> orderList() {
	      List<Pair> L = new ArrayList<>();
	      String f = field();
	      String k = "";
	      if (lex.matchKeyword("asc")) {
	    	  lex.eatKeyword("asc");
	    	  k = "asc";
	    	  L.add(new Pair(f, true));
	      } else if (lex.matchKeyword("desc")) {
	    	  lex.eatKeyword("desc");
	    	  k = "desc";
	    	  L.add(new Pair(f, false));
	      } 
	      
	      if (lex.matchDelim(',')) {
	    	  L.add(new Pair(f, true));
	    	  lex.eatDelim(',');
	    	  L.addAll(orderList());
	      }
	      
	      if (!k.equals("asc") || !k.equals("desc")) {
	    	  L.add(new Pair(f, true));
	      }
	      return L;
	   }
   
   private Collection<String> tableList() {
      Collection<String> L = new ArrayList<String>();
      L.add(lex.eatId());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(tableList());
      }
      return L;
   }
   
// Methods for parsing the various update commands
   
   public Object updateCmd() {
      if (lex.matchKeyword("insert"))
         return insert();
      else if (lex.matchKeyword("delete"))
         return delete();
      else if (lex.matchKeyword("update"))
         return modify();
      else
         return create();
   }
   
   private Object create() {
      lex.eatKeyword("create");
      System.out.println("Ate create");
      
      if (lex.matchKeyword("table"))
         return createTable();
      else if (lex.matchKeyword("view"))
         return createView();
      else
    	  System.out.println("Creating index");
         return createIndex();
   }
   
// Method for parsing delete commands
   
   public DeleteData delete() {
      lex.eatKeyword("delete");
      lex.eatKeyword("from");
      String tblname = lex.eatId();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new DeleteData(tblname, pred);
   }
   
// Methods for parsing insert commands
   
   public InsertData insert() {
      lex.eatKeyword("insert");
      lex.eatKeyword("into");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      List<String> flds = fieldList();
      lex.eatDelim(')');
      lex.eatKeyword("values");
      lex.eatDelim('(');
      List<Constant> vals = constList();
      lex.eatDelim(')');
      return new InsertData(tblname, flds, vals);
   }
   
// Methods for parsing queries
   
   public OrderData order() {
      lex.eatKeyword("order");
      lex.eatKeyword("by");
      List<Pair> fields = orderList();
      return new OrderData(fields);
   }
   
   private List<String> fieldList() {
      List<String> L = new ArrayList<String>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(fieldList());
      }
      return L;
   }
   
   private List<Constant> constList() {
      List<Constant> L = new ArrayList<Constant>();
      L.add(constant());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(constList());
      }
      return L;
   }
   
// Method for parsing modify commands
   
   public ModifyData modify() {
      lex.eatKeyword("update");
      String tblname = lex.eatId();
      lex.eatKeyword("set");
      String fldname = field();
      lex.eatDelim('=');
      Expression newval = expression();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }
      return new ModifyData(tblname, fldname, newval, pred);
   }
   
// Method for parsing create table commands
   
   public CreateTableData createTable() {
      lex.eatKeyword("table");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      Schema sch = fieldDefs();
      lex.eatDelim(')');
      return new CreateTableData(tblname, sch);
   }
   
   private Schema fieldDefs() {
      Schema schema = fieldDef();
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         Schema schema2 = fieldDefs();
         schema.addAll(schema2);
      }
      return schema;
   }
   
   private Schema fieldDef() {
      String fldname = field();
      return fieldType(fldname);
   }
   
   private Schema fieldType(String fldname) {
      Schema schema = new Schema();
      if (lex.matchKeyword("int")) {
         lex.eatKeyword("int");
         schema.addIntField(fldname);
      }
      else {
         lex.eatKeyword("varchar");
         lex.eatDelim('(');
         int strLen = lex.eatIntConstant();
         lex.eatDelim(')');
         schema.addStringField(fldname, strLen);
      }
      return schema;
   }
   
// Method for parsing create view commands
   
   public CreateViewData createView() {
      lex.eatKeyword("view");
      String viewname = lex.eatId();
      lex.eatKeyword("as");
      QueryData qd = query();
      return new CreateViewData(viewname, qd);
   }
   
   
//  Method for parsing create index commands
   
   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      System.out.println("Ate index");
      
      String indexKeyword = lex.eatIndexKeyword();
      System.out.println("Ate index keyword " + indexKeyword);
      
      String idxname = lex.eatId();
      System.out.println("Ate index name " + idxname);
      
      lex.eatKeyword("on");
      System.out.println("Ate on");
      
      String tblname = lex.eatId();
      System.out.println("Ate table name " + tblname);
      
      lex.eatDelim('(');
      System.out.println("Ate (");
      
      String fldname = field();
      System.out.println("Ate field name " + fldname);
      
      lex.eatDelim(')');
      System.out.println("Ate )");
      
      return new CreateIndexData(idxname, tblname, fldname, indexKeyword);
   }
}

