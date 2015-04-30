package com.distopik.wamp;

import static reactor.bus.selector.Selectors.$;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.registry.Registration;
import reactor.bus.selector.Selector;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class WebSocketAdapter extends org.eclipse.jetty.websocket.api.WebSocketAdapter {
	Logger log = LoggerFactory.getLogger(WebSocketAdapter.class);
	private static long sessionId = 0;
	private static final Map<String, EventBus> realmEventBus = new HashMap<>();
	private static final ObjectNode WAMP_ROUTER_CAPABILITIES = objectNode();
	static {
		WAMP_ROUTER_CAPABILITIES.set("agent", JsonNodeFactory.instance.textNode("reactor-wamp"));
		ObjectNode roles = objectNode();
		roles.set("broker", objectNode());
		roles.set("dealer", objectNode());
		WAMP_ROUTER_CAPABILITIES.set("roles", roles);
	}
	
	private static ObjectNode objectNode() {
		return JsonNodeFactory.instance.objectNode();
	}
	
	private String   realm     = "default";
	private EventBus realmBus  = null;
	
	private final Broadcaster<String>      strings  = Broadcaster.<String>create(Environment.cachedDispatcher());
	private final Broadcaster<byte[]>      bytes    = Broadcaster.<byte[]>create(Environment.cachedDispatcher());
	private final Broadcaster<Message> messages = Broadcaster.<Message>create(Environment.cachedDispatcher());
	
	private Map<Selector<?>, Registration<Consumer<? extends Event<?>>>> subs = new HashMap<>();
	
	private Function<byte[], JsonNode>    bytesDeserializer;
	private Function<String, JsonNode>    textDeserializer;
	private Function<Message, byte[]> bytesSerializer;
	private Function<Message, String> textSerializer;
	
	private boolean verifySecurity(Message msg) {
		return true;
	}
	
	private void cleanup() {
		for (Registration<Consumer<? extends Event<?>>> reg : subs.values()) {
			reg.cancel();
		}
		subs.clear();
	}
	
	private void dispatch(Message msg) {
		if (msg.getType() == Message.HELLO) {
			realm = msg.getUri();
			if (!realmEventBus.containsKey(realm))
				realmEventBus.put(realm, realmBus = EventBus.create());
			else realmBus = realmEventBus.get(realm);
			log.info("Client joined to realm '{}'", realm);
			messages.onNext(new Message(
					Message.WELCOME, 
					Long.toString(sessionId++), 
					WAMP_ROUTER_CAPABILITIES));
		} else if (msg.getType() == Message.SUBSCRIBE) {
			final String topic = msg.getUri();
			final Selector<?> selector = $(topic);
			log.info("Client wants to subscribe to '{}'", topic);
			if (subs.containsKey(selector)) {
				/* error */
			} else {
				subs.put(selector, realmBus.on(selector, (Event<JsonNode> event) -> {
					if (subs.containsKey(selector)) {
						messages.onNext(new Message(
								Message.EVENT,
								topic,
								event.getData()));
					}
				}));
			}
		} else if (msg.getType() == Message.UNSUBSCRIBE) {
			final String topic = msg.getUri();
			log.info("Client wants to unsubscribe from '{}'", topic);
			final Selector<?> selector = $(topic);
			if (subs.containsKey(selector)) {
				subs.remove(selector);
			}
		}
	}
	
	@Override
    public void onWebSocketConnect(Session session) {
    	super.onWebSocketConnect(session);
    	
    	log.info("CONNECTED {}", session.getUpgradeRequest().getSubProtocols());
		
		Stream<Message> messages  = createJsonStream()
										.map    (Message::create)       /* slice JSON to a message POJO           */
										.filter (Message::removeNulls)  /* remove all where parsing failed        */
										.filter (this::verifySecurity); /* skip msgs that the realm doesn't allow */
										
		consumeJsonStream(Streams.defer(() -> messages)               /* items come in when we publish msgs   */
								 .filter (unused  -> isConnected())); /* skip items when we are not connected */
		
		messages.consume(this::dispatch);
    }

	private void consumeJsonStream(Stream<Message> stream) {
		if (textSerializer != null) {
			stream.map    (textSerializer)
			      .consume(text -> getRemote().sendStringByFuture(text));
		} else {
			stream.map    (bytesSerializer)
		          .consume(bytes -> getRemote().sendBytesByFuture(ByteBuffer.wrap(bytes)));
		}
	}

	private Stream<JsonNode> createJsonStream() {
		if (textDeserializer != null)
			return Streams.defer(() -> strings)			/* items come in when we publish strings  */
					.filter (unused -> isConnected())   /* skip items when we are not connected   */
					.map    (textDeserializer);         /* convert strings to JSON                */
		else
			return Streams.defer(() -> bytes)           /* items come in when we publish strings  */
					.filter (unused -> isConnected())   /* skip items when we are not connected   */
					.map    (bytesDeserializer);        /* convert bytes to JSON                  */
	}
	
	private WebSocketAdapter() {}
	
	@Override
	public void onWebSocketClose(int statusCode, String reason) {
		super.onWebSocketClose(statusCode, reason);
		cleanup();
	}
	
	@Override
	public void onWebSocketBinary(byte[] payload, int offset, int len) {
		super.onWebSocketBinary(payload, offset, len);
		if (offset == 0 && len == payload.length) {
			bytes.onNext(payload);
		} else {
			byte[] xbytes = new byte[len];
			System.arraycopy(payload, offset, xbytes, 0, len);
			bytes.onNext(xbytes);
		}
	}
	
	@Override
	public void onWebSocketText(String message) {
		super.onWebSocketText(message);
		strings.onNext(message);
	}
	
	public static WebSocketAdapter createForText(final Function<String, JsonNode> textDeserializer, final Function<Message, String> textSerializer) {
		WebSocketAdapter rv = new WebSocketAdapter();
		rv.textDeserializer = textDeserializer;
		rv.textSerializer   = textSerializer;
		return rv;
	}
	
	public static WebSocketAdapter createForBytes(final Function<byte[], JsonNode> bytesDeserializer, final Function<Message, byte[]> bytesSerializer) {
		WebSocketAdapter rv = new WebSocketAdapter();
		rv.bytesDeserializer = bytesDeserializer;
		rv.bytesSerializer   = bytesSerializer;
		return rv;
	}
}
