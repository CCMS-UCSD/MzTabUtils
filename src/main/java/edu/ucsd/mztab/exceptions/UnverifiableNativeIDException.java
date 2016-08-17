package edu.ucsd.mztab.exceptions;

@SuppressWarnings("serial")
public class UnverifiableNativeIDException
extends Exception
{
	/*========================================================================
	 * Constructor
	 *========================================================================*/
	public UnverifiableNativeIDException(String message) {
		super(message);
	}
	
	public UnverifiableNativeIDException(String message, Throwable error) {
		super(message, error);
	}
}