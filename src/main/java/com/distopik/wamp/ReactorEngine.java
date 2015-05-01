package com.distopik.wamp;

import static reactor.bus.selector.Selectors.$;

import java.util.HashMap;
import java.util.Map;

import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.registry.Registration;
import reactor.fn.Consumer;
import reactor.fn.Function;

public class ReactorEngine implements Engine {
	
	static class RPCReg {
		
		public RPCReg(long registrationId, String uri, Invocation handler) {
			this.registrationId = registrationId;
			this.uri            = uri;
			this.handler        = handler;
		}
		
		long       registrationId;
		String     uri;
		Invocation handler;
	}
	
	static class Realm {
		Realm() {
			eventBus = EventBus.create();
		}
		
		EventBus            eventBus;
		Map<String, RPCReg> registrations  = new HashMap<>();
		long                registrationId = 1;
	}
	
	static class Session {
		Realm realm;
		long  subscriptionId = 1;
		long  publicationId  = 1;
		
		Map<Long, Registration<Consumer<? extends Event<?>>>> subscriptions = new HashMap  <>();
		Map<Long, RPCReg>                                     registrations = new HashMap  <>();
		
		public void cleanup() {
			synchronized (subscriptions) {
				for (Registration<Consumer<? extends Event<?>>> sub : subscriptions.values()) {
					sub.cancel();
				}
				subscriptions.clear();
				
				synchronized (realm.registrations) {
					for (RPCReg reg : registrations.values()) {
						realm.registrations.remove(reg.uri);
					}
				}
				registrations.clear();
			}
		}
	}
	
	Map<String, Realm> realms    = new HashMap<>();
	Map<Long, Session> sessions  = new HashMap<>();
	long               sessionId = 1;
	
	long checkSession(long sessionId, Function<Session, Long> func) {
		synchronized (sessions) {
			Session s = sessions.get(sessionId);
			if (s != null) {
				return func.apply(s); 
			} else {
				throw new IllegalArgumentException("sessionId");
			}
		}
	}
	
	long checkSubscription(Session session, long subscriptionId, Function<Registration<Consumer<? extends Event<?>>>, Long> func) {
		synchronized (session.subscriptions) {
			Registration<Consumer<? extends Event<?>>> sub = session.subscriptions.get(subscriptionId);
			if (sub != null) {
				return func.apply(sub);
			} else {
				throw new IllegalArgumentException("subscriptionId");
			}
		}
	}

	@Override
	public long createSession(String realm) {
		synchronized (sessions) {
			Session s = new Session();
			s.realm   = realms.get(realm);
			if (s.realm == null)
				realms.put(realm, s.realm = new Realm());
			
			long sId = sessionId++;
			sessions.put(sId, s);
			return sId;
		}
	}

	@Override
	public void closeSession(long sessionId) {
		checkSession(sessionId, s -> {
			s.cleanup();
			sessions.remove(sessionId);
			return 0L;
		});
	}

	@Override
	public long subscribe(long sessionId, String uri, Notification callme) {
		return checkSession(sessionId, s -> {
			long subId = s.subscriptionId++;
			s.subscriptions.put(subId, s.realm.eventBus.<Event<Message>>on($(uri), event -> {
				event.getData().setSubscriptionId(subId);
				if (!callme.notify(event.getData())) {
					checkSubscription(s, subId, sub -> {
						sub.cancel();
						s.subscriptions.remove(subId);
						return 0L;
					});
				}
			}));
			return subId;
		});
	}

	@Override
	public void unsubscribe(long sessionId, long subscriptionId) {
		checkSession(sessionId, s -> {
			checkSubscription(s, subscriptionId, sub -> {
				sub.cancel();
				s.subscriptions.remove(subscriptionId);
				return 0L;
			});
			return 0L;
		});
	}

	@Override
	public long publish(long sessionId, String uri, Message arguments) {
		return checkSession(sessionId, s -> {
			long pubId = s.publicationId++;
			arguments.setPublicationId(pubId);
			s.realm.eventBus.notify(uri, new Event<>(arguments));
			return pubId;
		});
	}

	@Override
	public long register(long sessionId, String uri, Invocation callme) {
		return checkSession(sessionId, s -> {
			synchronized (s.registrations) {
				synchronized (s.realm.registrations) {
					if (s.realm.registrations.containsKey(uri)) {
						throw new IllegalArgumentException("uri");
					}
					
					long regId = s.realm.registrationId++;
					RPCReg rpcReg = new RPCReg(regId, uri, callme);
					s.realm.registrations.put(uri, rpcReg);
					s.registrations.put(regId, rpcReg);
					return regId;
				}
			}
		});
	}

	@Override
	public void unregister(long sessionId, long registrationId) {
		checkSession(sessionId, s -> {
			synchronized (s.registrations) {
				RPCReg reg = s.registrations.get(registrationId);
				if (reg == null)
					throw new IllegalArgumentException("registrationId");
				synchronized (s.realm.registrations) {
					s.realm.registrations.remove(reg.uri);
					s.registrations.remove(registrationId);
				}
			}
			return 0L;
		});
	}

	@Override
	public void call(long sessionId, String uri, Message args, Notification callme) {
		checkSession(sessionId, s -> {
			synchronized (s.realm.registrations) {
				RPCReg reg = s.realm.registrations.get(uri);
				if (reg == null)
					throw new IllegalArgumentException("uri");
				
				args.setRegistrationId(reg.registrationId);
				reg.handler.invoke(args, callme);
				
				return 0L;
			}
		});
	}
}
