package simpledb.parse;

import java.util.*;

import simpledb.query.*;
import simpledb.record.*;

/**
 * The SimpleDB parser.
 * 
 * @author Edward Sciore
 */
public class Parser {
   private Lexer lex;

   public Parser(String s) {
      lex = new Lexer(s);
   }

   // Methods for parsing predicates, terms, expressions, constants, and fields

   /**
    * Function that eats a field: a value that is either an operator, a keyword, or
    * an index keyword
    * 
    * @return - the field that was consumed by the Lexer
    */
   public String field() {
      return lex.eatId();
   }

   /**
    * Function that eats a field (string or int) from the lexer and returns a
    * Constant object.
    * 
    * @return - the Constant object created from the consumed String or Integer
    */
   public Constant constant() {
      if (lex.matchStringConstant())
         return new Constant(lex.eatStringConstant());
      else
         return new Constant(lex.eatIntConstant());
   }

   /**
    * Function that eats a field or constant and returns an Expression.
    * 
    * @return the Expression object created from the consumed field or Constant
    */
   public Expression expression() {
      if (lex.matchId())
         return new Expression(field());
      else
         return new Expression(constant());
   }

   /**
    * Creates a new Term from 2 expressions and a conjoining operator
    * 
    * @return the Term object created from the consumed lhs expression, rhs
    *         expression and operator
    */
   public Term term() {
      Expression lhs = expression();
      Operator operator = lex.eatOperator();
      Expression rhs = expression();
      return new Term(lhs, rhs, operator);
   }

   /**
    * Creates a new Predicate - recursively calls itself to conjoin all terms in
    * the query into one single predicate.
    * 
    * @return the Predicate object created from the consumed terms
    */
   public Predicate predicate() {
      Predicate pred = new Predicate(term());
      if (lex.matchKeyword("and")) {
         lex.eatKeyword("and");
         pred.conjoinWith(predicate());
      }
      return pred;
   }

   // Methods for parsing queries

   /**
    * Creates a new QueryData from an input query string
    * 
    * @return - the QueryData object to be processed
    */
   public QueryData query() {

      boolean distinct = false;

      lex.eatKeyword("select");
      if (lex.matchKeyword("distinct")) {
         lex.eatKeyword("distinct");
         distinct = true;
      }

      List<String> fields = selectList();
      lex.eatKeyword("from");

      Collection<String> tables = tableList();
      Predicate pred = new Predicate();
      if (lex.matchKeyword("where")) {
         lex.eatKeyword("where");
         pred = predicate();
      }

      List<String> groupList = new ArrayList<String>();
      if (lex.matchKeyword("group")) {
         groupList = group();
      }

      OrderData od;
      if (lex.matchKeyword("order")) {
         od = order();
      } else {
         List<Pair> L = new ArrayList<>();
         for (String f : fields) {
            L.add(new Pair(f, true));
         }
         od = new OrderData(L);
      }

      return new QueryData(fields, tables, pred, groupList, od, distinct);
   }

   /**
    * Creates an ArrayList of Strings consisting of the attributes in the SELECT
    * Query
    * Uses "," as a delimiter to identify separate attributes in the SELECT query.
    * 
    * @return - the List of attribute names in the SELECT query
    */
   private List<String> selectList() {
      List<String> L = new ArrayList<>();
      L.add(field());
      if (lex.matchDelim(',')) {
         lex.eatDelim(',');
         L.addAll(selectList());
      }
      return L;
   }

   /**
    * Creates an ArrayList of Pairs consisting of the attributes in the ORDER BY
    * Query
    * Each Pair consists of an attribute name and the direction of ordering
    * (ascending or descending).
    * Checks whether the attribute is ordered by "asc" or "desc", with "asc" when
    * absent.
    * 
    * @return - the List of Pairs in the ORDER BY query
    */
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

   public List<String> group() {
      lex.eatKeyword("group");
      lex.eatKeyword("by");
      List<String> fields = fieldList();
      return fields;
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
      } else {
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

   // Method for parsing create index commands
   public CreateIndexData createIndex() {
      lex.eatKeyword("index");
      String indexKeyword = lex.eatIndexKeyword();
      String idxname = lex.eatId();
      lex.eatKeyword("on");
      String tblname = lex.eatId();
      lex.eatDelim('(');
      String fldname = field();
      lex.eatDelim(')');

      return new CreateIndexData(idxname, tblname, fldname, indexKeyword);
   }
}
