package com.distopik.wamp;

public interface Notification {
	/*
	 * return false if the invocation should be removed
	 */
	boolean notify(Arguments args);  
}
