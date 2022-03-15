package simpledb.query;

import java.util.Arrays;
import java.util.Collection;

import simpledb.parse.BadSyntaxException;

/**
 * Represents the operator used in SQL queries.
 */
public class Operator {
	/**
	 * Allowed operators.
	 */
	public static final Collection<String> operators = Arrays.asList("=", "<>", "!=", ">", ">=", "<", "<=");
	
	enum operatorType {
		EQUAL,
		NOT_EQUAL,
		GREATER_THAN,
		LESS_THAN,
		GREATER_OR_EQUAL,
		LESS_OR_EQUAL		
	}
	
	private final operatorType ownType;
	
	/**
	 * Creates an operator object given a string representing one of the allowed operators.
	 * @param s String of the operator. 
	 */
	public Operator(String s) {
		switch (s) {
		case "=":
			ownType = operatorType.EQUAL;
			break;
		case "<>":
		case "!=":
			ownType = operatorType.NOT_EQUAL;
			break;
		case ">":
			ownType = operatorType.GREATER_THAN;
			break;
		case ">=":
			ownType = operatorType.GREATER_OR_EQUAL;
			break;
		case "<":
			ownType = operatorType.LESS_THAN;
			break;
		case "<=":
			ownType = operatorType.LESS_OR_EQUAL;
			break;
		default:
			throw new BadSyntaxException();				
		}
		 
	}
	
	/**
	 * Operates on the LHS and RHS constant given the type of this operator.
	 * @param lhsval LHS constant
	 * @param rhsVal RHS constant
	 * @return result of the operation.
	 */
	public boolean operate(Constant lhsval, Constant rhsVal) {
		switch (ownType) {
		case EQUAL:
			return lhsval.equals(rhsVal);
		case NOT_EQUAL:
			return !lhsval.equals(rhsVal);
		case GREATER_THAN:
			return lhsval.compareTo(rhsVal) > 0;
		case LESS_THAN:
			return lhsval.compareTo(rhsVal) < 0;
		case LESS_OR_EQUAL:
			return lhsval.compareTo(rhsVal) < 0 || lhsval.equals(rhsVal);
		case GREATER_OR_EQUAL:
			return lhsval.compareTo(rhsVal) > 0 || lhsval.equals(rhsVal);
		default:
			throw new BadSyntaxException();					
		}	
	}
}	
