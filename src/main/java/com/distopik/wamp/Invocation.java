package com.distopik.wamp;

public interface Invocation {
	boolean invoke(Message args, Notification notifyWithResults);
}
