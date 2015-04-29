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

import java.util.List;
import java.util.ArrayList;

import reactor.bus.selector.Selector;

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
		String   uri;
		JsonNode payload;
		
		public static final int HELLO        = 1;
		public static final int WELCOME      = 2;
		public static final int ABORT        = 3;
		public static final int CHALLENGE    = 4;
		public static final int AUTHENTICATE = 5;
		public static final int GOODBYE      = 6;
		
		public static boolean verifyFormat(JsonNode node) {
			return node.isArray() && node.size() >= 2 && node.get(0).isIntegralNumber();
		}
		
		public WAMPMessage(JsonNode node) {
			this.type    = node.get(0).asLong();
			this.uri     = node.get(1).asText();
			this.payload = node.get(2);
		}
		
		public WAMPMessage(int type, String uri, JsonNode payload) {
			this.type    = type;
			this.uri     = uri;
			this.payload = payload;
		}
		
		public static WAMPMessage create(JsonNode node) {
			return new WAMPMessage(node);
		}
		
		public String serialize() {
			ArrayNode rv = JsonNodeFactory.instance.arrayNode();
			rv.add(type);
			if (uri != null)
				rv.add(uri);
			if (payload != null)
				rv.add(payload);
			
			return rv.toString();
		}
		
		public long getType() {
			return type;
		}
		
		public String getUri() {
			return uri;
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
						ObjectMapper             mapper   = new ObjectMapper();
						Broadcaster<String>      strings  = Broadcaster.<String>create(Environment.cachedDispatcher());
						Broadcaster<WAMPMessage> messages = Broadcaster.<WAMPMessage>create(Environment.cachedDispatcher());
						List<Selector<String>>   subs     = new ArrayList<>();
						
						private boolean verifySecurity(WAMPMessage node) {
							return true;
						}
						
						private void dispatch(WAMPMessage msg) {
							if (msg.getType() == WAMPMessage.HELLO) {
								messages.onNext(new WAMPMessage(
										WAMPMessage.WELCOME, 
										msg.getUri(), 
										JsonNodeFactory.instance.objectNode()));
							}
						}
						
						private JsonNode readJson(String string) {
							try {
								return mapper.readTree(string);
							} catch (Exception e) { return null; }
						}
						
						@Override
						public void onWebSocketBinary(byte[] payload, int offset, int len) {
							super.onWebSocketBinary(payload, offset, len);
							log.info("+BYTES: '{}' @{}+{}", payload, offset, len);
						}

						@Override
						public void onWebSocketClose(int statusCode,
								String reason) {
							super.onWebSocketClose(statusCode, reason);
							log.info("CLOSED {} because '{}'", statusCode, reason);
						}

						@Override
						public void onWebSocketConnect(Session session) {
							super.onWebSocketConnect(session);
							log.info("CONNECTED {}", session.getUpgradeRequest().getSubProtocols());
							
							Stream<WAMPMessage> receiving = Streams.defer(() -> strings)
															.filter (unused -> isConnected())
															.map    (this::readJson)
															.filter (WAMPMessage::verifyFormat)
															.map    (WAMPMessage::create)
															.filter (this::verifySecurity);
															
							Stream<String>      sending   = Streams.defer(() -> messages)
															.filter (unused  -> isConnected())
															.map    (message -> message.serialize());
							
							receiving.consume(this::dispatch);
							sending.consume(msgString -> getRemote().sendStringByFuture(msgString));
						}

						@Override
						public void onWebSocketError(Throwable cause) {
							super.onWebSocketError(cause);
							log.error("ERROR", cause);
							if (isConnected()) {
								getSession().close();
							}
						}

						@Override
						public void onWebSocketText(String message) {
							super.onWebSocketText(message);
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