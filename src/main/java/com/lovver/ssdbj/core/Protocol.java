package com.lovver.ssdbj.core;

import com.lovver.ssdbj.exception.SSDBException;

public interface Protocol {
	String getProtocol();

	String getProtocolVersion();

	CommandExecutor getCommandExecutor();

	void auth() throws SSDBException;
}
