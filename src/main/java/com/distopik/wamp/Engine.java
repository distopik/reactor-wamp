package com.distopik.wamp;

public interface Engine {
	long createSession(String realm);
	void closeSession (long   sessionId);
	long subscribe    (long   sessionId, String uri, Notification callme);
	long publish      (long   sessionId, String uri, Arguments    args);
	void call         (long   sessionId, String uri, Arguments    args, Notification callme);
	long register     (long   sessionId, String uri, Invocation   callme);
	void unsubscribe  (long   sessionId, long subscriptionId);
	void unregister   (long   sessionId, long registrationId);
}
