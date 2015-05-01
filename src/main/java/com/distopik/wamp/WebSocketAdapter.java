package com.distopik.wamp;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import reactor.Environment;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple2;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


import static com.distopik.wamp.Message.*;

public class WebSocketAdapter extends org.eclipse.jetty.websocket.api.WebSocketAdapter {
	Logger log = LoggerFactory.getLogger(WebSocketAdapter.class);
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
	
	private Engine engine       = null;
	private long   sessionId    = -1;
	private long   invocationId = 1;
	
	private final Broadcaster<String>  strings  = Broadcaster.<String> create(Environment.cachedDispatcher());
	private final Broadcaster<byte[]>  bytes    = Broadcaster.<byte[]> create(Environment.cachedDispatcher());
	private final Broadcaster<Message> replies  = Broadcaster.<Message>create(Environment.cachedDispatcher());
	
	private Function<byte[], JsonNode> bytesDeserializer;
	private Function<String, JsonNode> textDeserializer;
	private Function<Message, byte[]>  bytesSerializer;
	private Function<Message, String>  textSerializer;
	private Map<Long, Notification>    futureInvocations = new HashMap<>();
	
	private Message debugMessage(Message msg) {
		log.info(MessageSpec.debug(msg));
		return msg;
	}
	
	/* used sometimes :) */
	@SuppressWarnings("unused")
	private void dispatch(final List<Tuple2<Long, Message>> block) {
		for (Tuple2<Long, Message> tuple : block) {
			dispatchSingle(tuple);
		}
	}

	private void dispatchSingle(Tuple2<Long, Message> tuple) {
		final Message msg = tuple.getT2();
		guardErrors(msg, unused -> {
			log.info("{}", tuple.getT1());
			
			if (engine == null) {
				throw new IllegalStateException("No engine");
			}
			
			if (sessionId < 0 && msg.getType() != HELLO) {
				throw new IllegalStateException("First message must be HELLO");
			}
			
			if (msg.getType() == ERROR) {
				getSession().close();
			} else if (msg.getType() == HELLO) {
				onHello(msg);
			} else if (msg.getType() == SUBSCRIBE) {
				onSubscribe(msg);
			} else if (msg.getType() == PUBLISH) {
				onPublish(msg);
			} else if (msg.getType() == REGISTER) {
				onRegister(msg);
			} else if (msg.getType() == YIELD) {
				onYield(msg);
			}
		});
	}
	
	public void guardErrors(Message msg, Consumer<Message> func) {
		try {
			func.accept(msg);
		} catch (Exception e) {
			Message error = new Message(ERROR, msg);
			error.setDetails(JsonNodeFactory.instance.objectNode().put("exception", e.toString()));
			queueReply(error);
		}
	}
	
	private void queueReply(final Message reply) {
		replies.onNext(reply);
	}
	
	public void onHello(final Message msg) {
		guardErrors(msg, unused-> {
			if (sessionId > 0) {
				throw new IllegalStateException("already welcome");
			}
			
			final Message reply = new Message(WELCOME, msg);
			reply.setSessionId(sessionId = engine.createSession(msg.getUri()));
			reply.setDetails(WAMP_ROUTER_CAPABILITIES);
			queueReply(reply);
		});
	}
	
	public void onSubscribe(final Message msg) {
		guardErrors(msg, unused -> {
			long subId = engine.subscribe(sessionId, msg.getUri(), received -> {
				if (isConnected()) {
					Message eventMessage = new Message(EVENT, received);
					queueReply(eventMessage);
					return true;
				}
				return false;
			});
			
			final Message reply = new Message(SUBSCRIBED, msg);
			reply.setSubscriptionId(subId);
			queueReply(reply);
		});
	}

	public void onPublish(final Message msg) {
		guardErrors(msg, unused -> {
			long pubId = engine.publish(sessionId, msg.getUri(), msg);
			if (msg.getDetails().has("acknowledge") && msg.getDetails().get("acknowledge").asBoolean()) {
				final Message reply = new Message(PUBLISHED, msg);
				reply.setPublicationId(pubId);
				queueReply(reply);
			}
		});
	}
	
	private void onRegister(Message msg) {
		guardErrors(msg, unused -> {
			long regId = engine.register(sessionId, msg.getUri(), (args, notify) -> {
				if (isConnected()) {
					synchronized (futureInvocations) {
						Message invoke = new Message(INVOCATION, args);
						long invId = invocationId++;
						invoke.setRequestId(invId);
						queueReply(invoke);
						futureInvocations.put(invId, notify);
						return true;
					}
				} else {
					return false;
				}
			});
			
			Message registered = new Message(REGISTERED, msg);
			registered.setRegistrationId(regId);
			queueReply(registered);
		});
	}
	
	private void onYield(Message msg) {
		guardErrors(msg, unused -> {
			synchronized (futureInvocations) {
				Notification target = futureInvocations.get(msg.getRequestId());
				if (target == null)
					throw new IllegalArgumentException("requestId");
				
				futureInvocations.remove(msg.getRequestId());
				target.notify(msg);
			}
		});
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
			.consume  (this::dispatchSingle);           /* then dispatch the whole lot 				*/
								
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
		engine.closeSession(sessionId);
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
	
	public static WebSocketAdapter createForText(final Engine engine, final Function<String, JsonNode> textDeserializer, final Function<Message, String> textSerializer) {
		WebSocketAdapter rv = new WebSocketAdapter();
		rv.textDeserializer = textDeserializer;
		rv.textSerializer   = textSerializer;
		rv.engine           = engine;
		return rv;
	}
	
	public static WebSocketAdapter createForBytes(final Engine engine, final Function<byte[], JsonNode> bytesDeserializer, final Function<Message, byte[]> bytesSerializer) {
		WebSocketAdapter rv = new WebSocketAdapter();
		rv.bytesDeserializer = bytesDeserializer;
		rv.bytesSerializer   = bytesSerializer;
		rv.engine            = engine;
		return rv;
	}
}
