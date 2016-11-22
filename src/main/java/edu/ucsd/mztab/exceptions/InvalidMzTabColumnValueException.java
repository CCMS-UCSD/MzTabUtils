package edu.ucsd.mztab.exceptions;

@SuppressWarnings("serial")
public class InvalidMzTabColumnValueException
extends Exception
{
	/*====================================================================
	 * Constructor
	 *====================================================================*/
	public InvalidMzTabColumnValueException(String message) {
		super(message);
	}
}
