package com.distopik.wamp;

import static reactor.bus.selector.Selectors.$;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import reactor.fn.tuple.Tuple2;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class WebSocketAdapter extends org.eclipse.jetty.websocket.api.WebSocketAdapter {
	Logger log = LoggerFactory.getLogger(WebSocketAdapter.class);
	private static long sessionId = 0;
	private static long subscriptionId = 0;
	
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
	
	private <T> boolean filterNulls(T item) {
		return item != null;
	}
	
	private Message readMessage(JsonNode node) {
		if (!node.isArray()) {
			return null; /* this can become an error at some point .. */
		}
		
		return new Message(node);
	}
	
	private String   realm     = "default";
	private EventBus realmBus  = null;
	
	private final Broadcaster<String>  strings  = Broadcaster.<String> create(Environment.cachedDispatcher());
	private final Broadcaster<byte[]>  bytes    = Broadcaster.<byte[]> create(Environment.cachedDispatcher());
	private final Broadcaster<Message> replies  = Broadcaster.<Message>create(Environment.cachedDispatcher());
	
	private Map<Selector<?>, Registration<Consumer<? extends Event<?>>>> subs = new HashMap<>();
	
	private Function<byte[], JsonNode> bytesDeserializer;
	private Function<String, JsonNode> textDeserializer;
	private Function<Message, byte[]>  bytesSerializer;
	private Function<Message, String>  textSerializer;
	
	private void cleanup() {
		for (Registration<Consumer<? extends Event<?>>> reg : subs.values()) {
			reg.cancel();
		}
		subs.clear();
	}
	
	private Message debugMessage(Message msg) {
		log.info(MessageSpec.debug(msg));
		return msg;
	}
	
	private void dispatch(final List<Tuple2<Long, Message>> block) {
		int i = 0;
		for (Tuple2<Long, Message> tuple : block) {
			final Message msg = tuple.getT2();
			log.info("{}: {}", i++, tuple.getT1());
			if (msg.getType() == Message.HELLO) {
				onHello(msg);
			} else if (msg.getType() == Message.SUBSCRIBE) {
				onSubscribe(msg);
			} else if (msg.getType() == Message.PUBLISH) {
				onPublish(msg);
			}
		}
	}

	public void onPublish(final Message msg) {
		final Message reply = new Message(Message.PUBLISHED, msg);
		final Selector<?> s = $(msg.getUri());
		realmBus.send(s, new Event<Message>(msg));
		replies.onNext(reply);
	}

	public void onSubscribe(final Message msg) {
		final Message reply = new Message(Message.SUBSCRIBED, msg);
		final Selector<?> s = $(msg.getUri());
		synchronized (subs) {
			if (!subs.containsKey(s)) {
				final long subId = subscriptionId++;
				reply.setSubscriptionId(subId);
				subs.put(s, realmBus.<Event<Message>>on(s, event -> {
					Message eventMessage = new Message(Message.EVENT, event.getData());
					eventMessage.setSubscriptionId(subId);
					replies.onNext(eventMessage);
				}));
			}
		}
		replies.onNext(reply);
	}

	public void onHello(final Message msg) {
		final Message reply = new Message(Message.WELCOME, msg);
		this.realm    = msg.getUri();
		synchronized (realmEventBus) {
			this.realmBus = realmEventBus.get(realm);
			if (this.realmBus == null)
				realmEventBus.put(realm, this.realmBus = EventBus.create());
		}
		reply.setSessionId(sessionId++);
		reply.setDetails(WAMP_ROUTER_CAPABILITIES);
		replies.onNext(reply);
	}
	
	@Override
    public void onWebSocketConnect(Session session) {
    	super.onWebSocketConnect(session);
    	
    	log.info("CONNECTED {}", session.getUpgradeRequest().getSubProtocols());
		
		createJsonStream()
			.map      (this::readMessage)     			/* slice JSON to a message POJO          	*/
			.map      (this::debugMessage)    			/* debugging                              	*/
			.filter   (this::filterNulls)     			/* remove all where parsing failed        	*/
			.timestamp()                              	/* apply timestamp to msgs    				*/
			.buffer   (250, TimeUnit.MILLISECONDS) 		/* buffer 25 or for 10ms      				*/
			.consume  (this::dispatch);               	/* then dispatch the whole lot 				*/
								
		consumeJsonStream(Streams.defer(() -> replies)                /* items come in when we publish msgs   */
								 .map    (this::debugMessage)         /* debugging                            */
								 .filter (unused  -> isConnected())); /* skip items when we are not connected */
    }

	private void consumeJsonStream(Stream<Message> stream) { /* FnF */
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
