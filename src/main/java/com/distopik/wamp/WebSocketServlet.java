package com.distopik.wamp;

import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

public class WebSocketServlet extends org.eclipse.jetty.websocket.servlet.WebSocketServlet {
	private static final long serialVersionUID = 1L;
	private static final String WAMP_JSON_V2    = "wamp.v2.json";
	private static final String WAMP_MSGPACK_V2 = "wamp.v2.msgpack";

	@Override
	public void configure(WebSocketServletFactory factory) {
		factory.setCreator((req, resp) -> {
			if (req.getSubProtocols().contains(WAMP_JSON_V2)) {
				resp.setAcceptedSubProtocol(WAMP_JSON_V2);
				return WebSocketAdapter.createForText(Codecs::readJson, Codecs::writeJson);
			} else if (req.getSubProtocols().contains(WAMP_MSGPACK_V2)) {
				resp.setAcceptedSubProtocol(WAMP_MSGPACK_V2);
				return WebSocketAdapter.createForBytes(Codecs::readMsgpack, Codecs::writeMsgpack);
			} else {
				resp.setSuccess(false);
				return null;
			}
		});
	}
}
