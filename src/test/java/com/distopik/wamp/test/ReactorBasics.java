package com.distopik.wamp.test;

import javax.servlet.http.HttpServletRequest;

import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketServlet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;


public class ReactorBasics {
	private static class Trade {
		
	}
	
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
		final EventBus serverReactor = EventBus.create(env);
		final Selector<String> tradeExecute = Selectors.object("trade.execute");
		serverReactor.on(tradeExecute, (Event<Trade> event) -> {
			System.out.println("event!");
		});
		
		WebSocketServlet wss = new WebSocketServlet() {
			private static final long serialVersionUID = 1L;

			@Override
			public WebSocket doWebSocketConnect(HttpServletRequest request, String protocol) {
				System.out.println("CONNECTED: protocol: " + protocol);
				return null;
			}
		};
	}
}