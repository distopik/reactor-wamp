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
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.Environment;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

public class ReactorBasics {
	private static Server server;
	Logger log = LoggerFactory.getLogger(ReactorBasics.class);
	
	static {
		Environment.initialize();
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
	
	static class WAMPMessage {
		long     type;
		JsonNode payload;
		
		public static boolean verifyFormat(JsonNode node) {
			return node.isArray() && node.size() >= 2 && node.get(0).isIntegralNumber();
		}
		
		public WAMPMessage(JsonNode node) {
			this.type    = node.get(0).asLong();
			this.payload = node.get(1); 
		}
		
		public static WAMPMessage create(JsonNode node) {
			return new WAMPMessage(node);
		}
		
		public String serialize() {
			ArrayNode rv = JsonNodeFactory.instance.arrayNode();
			rv.add(type);
			rv.add(payload);
			
			return rv.toString();
		}
	}
	
	@Test
	public void simple() throws Exception {
		WebSocketServlet wss = new WebSocketServlet() {
			private static final long serialVersionUID = 1L;

			@Override
			public void configure(WebSocketServletFactory factory) {
				factory.setCreator((req, resp) -> {
					resp.setAcceptedSubProtocol(req.getSubProtocols().get(0));
					
					return new WebSocketAdapter() {
						ObjectMapper        mapper  = new ObjectMapper();
						Broadcaster<String> strings = Broadcaster.<String>create(Environment.cachedDispatcher());
						
						private boolean verifySecurity(WAMPMessage node) {
							return true;
						}
						
						private void dispatch(Session session, WAMPMessage msg) {
							
						}
						
						private JsonNode readJson(String string) {
							try {
								return mapper.readTree(string);
							} catch (Exception e) { return null; }
						}
						
						@Override
						public void onWebSocketBinary(byte[] payload, int offset, int len) {
							log.info("+BYTES: '{}' @{}+{}", payload, offset, len);
						}

						@Override
						public void onWebSocketClose(int statusCode,
								String reason) {
							log.info("CLOSED {} because '{}'", statusCode, reason);
						}

						@Override
						public void onWebSocketConnect(Session session) {
							super.onWebSocketConnect(session);
							log.info("CONNECTED {}", session.getUpgradeRequest().getSubProtocols());
							
							/**
							 * Stream progression:
							 * 
							 *  - convert to JSON
							 *  - filter 
							 */
							Stream<WAMPMessage> receiving = Streams.defer(() -> strings)
															.filter (unused -> session.isOpen())
															.map    (this::readJson)
															.filter (WAMPMessage::verifyFormat)
															.map    (WAMPMessage::create)
															.filter (this::verifySecurity);
															
							Stream<WAMPMessage> messages  = Broadcaster.<WAMPMessage>create(Environment.cachedDispatcher());
							Stream<String>      sending   = Streams.defer(() -> messages)
															.filter (unused  -> session.isOpen())
															.map    (message -> message.serialize());
							
							receiving.consume(msg -> dispatch(session, msg));
							sending.consume(msgString -> session.getRemote().sendStringByFuture(msgString));
						}

						@Override
						public void onWebSocketError(Throwable cause) {
							log.error("ERROR", cause);
						}

						@Override
						public void onWebSocketText(String message) {
							// submit our bytes to the stream
							log.info("+TEXT: '{}'", message);
							strings.onNext(message);
						}
					};
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

		try (ServerConnector connector = new ServerConnector(server,
				Executors.newFixedThreadPool(4),
				new ScheduledExecutorScheduler(), new MappedByteBufferPool(),
				1, 4, connFac)) {
			connector.setAcceptQueueSize(1000);
			connector.setReuseAddress(true);
		}

		server.setHandler(handler);
		server.start();
	}
}