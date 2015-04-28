package com.distopik.wamp.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.Environment;


public class ReactorBasics {
	private Environment env;
	
	@Before
	public void setupReactor() {
		env = new Environment();
	}
	
	@After
	public void teardownReactor() {
		env.shutdown();
		env = null;
	}
	
	@Test
	public void simple() {
	}
}