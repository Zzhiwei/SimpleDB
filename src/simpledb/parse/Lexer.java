package simpledb.parse;

import static simpledb.query.Operator.operators;

import java.util.*;

import simpledb.query.Operator;

import java.io.*;

/**
 * The lexical analyzer.
 * @author Edward Sciore
 */
public class Lexer {
   private Collection<String> keywords;   
   private Collection<String> indexKeywords;
   private StreamTokenizer tok;
   
   /**
    * Creates a new lexical analyzer for SQL statement s.
    * @param s the SQL statement
    */
   public Lexer(String s) {
      initKeywords();
      initIndexKeywords();
      tok = new StreamTokenizer(new StringReader(s));
      tok.ordinaryChar('.');   //disallow "." in identifiers
      tok.wordChars('_', '_'); //allow "_" in identifiers
      tok.wordChars('<', '<'); //allow "<" in identifiers
      tok.wordChars('=', '='); //allow "=" in identifiers
      tok.wordChars('>', '>'); //allow ">" in identifiers
      tok.lowerCaseMode(true); //id and keywords are converted
      nextToken();
   }
   
//Methods to check the status of the current token
   
   /**
    * Returns true if the current token is
    * the specified delimiter character.
    * @param d a character denoting the delimiter
    * @return true if the delimiter is the current token
    */
   public boolean matchDelim(char d) {
      return d == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is an integer.
    * @return true if the current token is an integer
    */
   public boolean matchIntConstant() {
      return tok.ttype == StreamTokenizer.TT_NUMBER;
   }
   
   /**
    * Returns true if the current token is a string.
    * @return true if the current token is a string
    */
   public boolean matchStringConstant() {
      return '\'' == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is the specified keyword.
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchKeyword(String w) {
      return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
   }
   
   /**
    * Returns true if current token is an index keyword.
    * @return true if current token is an index keyword
    */
   public boolean matchIndexKeyword() {
	   return tok.ttype == StreamTokenizer.TT_WORD && indexKeywords.contains(tok.sval);
   }
   
   /**
    * Returns true if the current token is a legal identifier.
    * @return true if the current token is an identifier
    */
   public boolean matchId() {
      return  tok.ttype==StreamTokenizer.TT_WORD && !keywords.contains(tok.sval) && !operators.contains(tok.sval) && !indexKeywords.contains(tok.sval);
   }
   
   /**
    * Returns true if the current token is an operator.
    * @return true if that operator is the current token
    */
   public boolean matchOperator() {
	   return tok.ttype==StreamTokenizer.TT_WORD && operators.contains(tok.sval);
   }
   
//Methods to "eat" the current token
   
   /**
    * Throws an exception if the current token is not the
    * specified delimiter. 
    * Otherwise, moves to the next token.
    * @param d a character denoting the delimiter
    */
   public void eatDelim(char d) {
      if (!matchDelim(d))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not 
    * an integer. 
    * Otherwise, returns that integer and moves to the next token.
    * @return the integer value of the current token
    */
   public int eatIntConstant() {
      if (!matchIntConstant())
         throw new BadSyntaxException();
      int i = (int) tok.nval;
      nextToken();
      return i;
   }
   
   /**
    * Throws an exception if the current token is not 
    * a string. 
    * Otherwise, returns that string and moves to the next token.
    * @return the string value of the current token
    */
   public String eatStringConstant() {
      if (!matchStringConstant())
         throw new BadSyntaxException();
      String s = tok.sval; //constants are not converted to lower case
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not the
    * specified keyword. 
    * Otherwise, moves to the next token.
    * @param w the keyword string
    */
   public void eatKeyword(String w) {
      if (!matchKeyword(w))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not an
    * index keyword. 
    * Otherwise, moves to the next token.
    * @return the string value of the current index keyword
    */
   public String eatIndexKeyword() {
	   if (!matchIndexKeyword()) {
		   throw new BadSyntaxException();
	   }
	   String s = tok.sval;
	   nextToken();
	   return s;
   }
   
   /**
    * Throws an exception if the current token is not a
    * valid identifier. 
    * Otherwise, moves to the next token.
    * @return the string value of the current identifier
    */
   public String eatId() {
      if (!matchId())
         throw new BadSyntaxException();
      
      String s = tok.sval;
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not
    * an operator.
    * Otherwise, moves to the next token.
    * @return the string value of the current operator
    */
   public Operator eatOperator() {
	   if (!matchOperator())
		   throw new BadSyntaxException();
	   String s = tok.sval;
	   nextToken();
	   return new Operator(s);
   }
   
   private void nextToken() {
      try {
         tok.nextToken();
      }
      catch(IOException e) {
         throw new BadSyntaxException();
      }
   }
   
   private void initKeywords() {
      keywords = Arrays.asList("select", "from", "where", "and",
                               "insert", "into", "values", "delete", "update", "set", 
                               "create", "table", "int", "varchar", "view", "as", "index", 
                               "on", "order", "by", "asc", "desc", "group", "distinct");
   }
   
   private void initIndexKeywords() {
	   indexKeywords = Arrays.asList("hash", "btree");
   }
}