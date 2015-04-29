package com.distopik.wamp.test;

import java.util.HashMap;
import java.util.Map;
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
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selector;
import reactor.bus.registry.Registration;

import static reactor.bus.selector.Selectors.$;

import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import reactor.fn.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
	
	private static final ObjectNode WAMP_ROUTER_CAPABILITIES = JsonNodeFactory.instance.objectNode();
	static {
		WAMP_ROUTER_CAPABILITIES.set("agent", JsonNodeFactory.instance.textNode("reactor-wamp"));
		ObjectNode roles = JsonNodeFactory.instance.objectNode();
		roles.set("broker", JsonNodeFactory.instance.objectNode());
		roles.set("dealer", JsonNodeFactory.instance.objectNode());
		WAMP_ROUTER_CAPABILITIES.set("roles", roles);
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
		
		public static final int SUBSCRIBE    = 32;
		public static final int UNSUBSCRIBE  = 32;
		public static final int EVENT        = 36;
		
		
		public static boolean verifyFormat(JsonNode node) {
			return node.isArray() && node.size() >= 2 && node.get(0).isIntegralNumber();
		}
		
		public WAMPMessage(JsonNode node) {
			/* very much not true ... fix at some point */
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
	
	static Map<String, EventBus> realmEventBus = new HashMap<>();
	
	@Test
	public void simple() throws Exception {
		WebSocketServlet wss = new WebSocketServlet() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public void configure(WebSocketServletFactory factory) {
				factory.setCreator((req, resp) -> {
					resp.setAcceptedSubProtocol(req.getSubProtocols().get(0));
					return new WebSocketAdapter() {
						long     sessionId = 0;
						String   realm     = "default";
						EventBus realmBus  = null;
						
						
						ObjectMapper               mapper   = new ObjectMapper();
						Broadcaster<String>        strings  = Broadcaster.<String>create(Environment.cachedDispatcher());
						Broadcaster<WAMPMessage>   messages = Broadcaster.<WAMPMessage>create(Environment.cachedDispatcher());
						
						Map<Selector<String>, Registration<Consumer<? extends Event<?>>>> subs = new HashMap<>();
						
						private boolean verifySecurity(WAMPMessage node) {
							return true;
						}
						
						private void dispatch(WAMPMessage msg) {
							if (msg.getType() == WAMPMessage.HELLO) {
								realm = msg.getUri();
								if (!realmEventBus.containsKey(realm))
									realmEventBus.put(realm, realmBus = EventBus.create());
								else realmBus = realmEventBus.get(realm);
								log.info("Client joined to realm '{}'", realm);
								messages.onNext(new WAMPMessage(
										WAMPMessage.WELCOME, 
										Long.toString(sessionId++), 
										WAMP_ROUTER_CAPABILITIES));
							} else if (msg.getType() == WAMPMessage.SUBSCRIBE) {
								final String topic = msg.getUri();
								final Selector<String> selector = $(topic);
								log.info("Client wants to subscribe to '{}'", topic);
								if (subs.containsKey(selector)) {
									/* error */
								} else {
									subs.put(selector, realmBus.on(selector, (Event<JsonNode> event) -> {
										if (subs.containsKey(selector)) {
											messages.onNext(new WAMPMessage(
													WAMPMessage.EVENT,
													topic,
													event.getData()));
										}
									}));
								}
							} else if (msg.getType() == WAMPMessage.UNSUBSCRIBE) {
								final String topic = msg.getUri();
								log.info("Client wants to unsubscribe from '{}'", topic);
								final Selector<String> selector = $(topic);
								if (subs.containsKey(selector)) {
									subs.remove(selector);
								}
							}
						}
						
						private void cleanup() {
							for (Registration<Consumer<? extends Event<?>>> reg : subs.values()) {
								reg.cancel();
							}
							subs.clear();
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
							cleanup();
						}

						@Override
						public void onWebSocketConnect(Session session) {
							super.onWebSocketConnect(session);
							log.info("CONNECTED {}", session.getUpgradeRequest().getSubProtocols());
							
							Stream<WAMPMessage> receiving = Streams.defer(() -> strings)        /* items come in when we publish strings  */
															.filter (unused -> isConnected())   /* skip items when we are not connected   */
															.map    (this::readJson)            /* convert strings to JSON                */
															.filter (WAMPMessage::verifyFormat) /* verify the format of JSON (array..)    */
															.map    (WAMPMessage::create)       /* slice JSON to a message POJO           */
															.filter (this::verifySecurity);     /* skip msgs that the realm doesn't allow */
															
							Stream<String>      sending   = Streams.defer(() -> messages)             /* items come in when we publish msgs   */
															.filter (unused  -> isConnected())        /* skip items when we are not connected */
															.map    (message -> message.serialize()); /* convert messages to strings          */
							
							receiving.consume(this::dispatch);
							sending.consume(msgString -> getRemote().sendStringByFuture(msgString));
						}

						@Override
						public void onWebSocketError(Throwable cause) {
							super.onWebSocketError(cause);
							log.error("ERROR", cause);
							if (isConnected()) {
								cleanup();
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