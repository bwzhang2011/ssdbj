package com.lovver.ssdbj.core.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import com.lovver.ssdbj.core.Protocol;
import com.lovver.ssdbj.core.protocol.SSDBProtocolImpl;

import jodd.util.StringUtil;

public class ProtocolFactory {

	public static Protocol createSSDBProtocolImpl(String protocolName, OutputStream os, InputStream is,
			Properties infos) {
		if (StringUtil.isEmpty(protocolName) || "ssdb".equals(protocolName.toLowerCase())) {
			return new SSDBProtocolImpl(os, is, infos);
		}
		throw new RuntimeException("not support prototol:" + protocolName);
	}
}
