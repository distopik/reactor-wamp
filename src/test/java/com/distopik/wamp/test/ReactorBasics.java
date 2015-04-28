package com.distopik.wamp.test;

import java.util.concurrent.Executors;

import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.io.MappedByteBufferPool;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;

public class ReactorBasics {
	private static Server server;
	Logger log = LoggerFactory.getLogger(ReactorBasics.class);
	
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
	public void simple() throws Exception {
		final EventBus serverReactor = EventBus.create(env);
		final Selector<String> tradeExecute = Selectors.object("trade.execute");
		serverReactor.on(tradeExecute, (Event<Trade> event) -> {
			System.out.println("event!");
		});

		WebSocketServlet wss = new WebSocketServlet() {
			private static final long serialVersionUID = 1L;

			@Override
			public void configure(WebSocketServletFactory factory) {
				factory.setCreator((req, resp) -> new WebSocketListener() {

					@Override
					public void onWebSocketBinary(byte[] payload, int offset, int len) {
					}

					@Override
					public void onWebSocketClose(int statusCode, String reason) {
					}

					@Override
					public void onWebSocketConnect(Session session) {
						log.info("CONNECTED {}", session.getUpgradeRequest().getSubProtocols());
					}

					@Override
					public void onWebSocketError(Throwable cause) {
					}

					@Override
					public void onWebSocketText(String message) {
						log.info("PAYLOAD: '" + message + "'");
					}
				});
			}
		};
		
		serve(wss);
		System.in.read();
		server.stop();
	}

	private static void serve(HttpServlet servlet) throws Exception {
		ServletHandler handler = new ServletHandler();
		handler.addServletWithMapping(new ServletHolder(servlet), "/");

		HttpConfiguration httpConfig = new HttpConfiguration();
		httpConfig.setOutputBufferSize(32 * 1024);
		httpConfig.setRequestHeaderSize(8 * 1024);
		httpConfig.setResponseHeaderSize(8 * 1024);
		httpConfig.setSendDateHeader(true);

		HttpConnectionFactory connFac = new HttpConnectionFactory(httpConfig);

		server = new Server(3000);

		ServerConnector connector = new ServerConnector(server,
				Executors.newFixedThreadPool(4),
				new ScheduledExecutorScheduler(), new MappedByteBufferPool(),
				1, 4, connFac);
		connector.setAcceptQueueSize(1000);
		connector.setReuseAddress(true);

		server.setHandler(handler);
		server.start();
	}
}