package com.lovver.ssdbj.core;

import com.lovver.ssdbj.exception.SSDBException;

public interface Protocol {
	int CHECK_SIZE = 2;
	
	int SEND_BUFFER = 4096;
	
	int READ_BUFFER = 8192;
	
	String getProtocol();

	String getProtocolVersion();

	CommandExecutor getCommandExecutor();

	void auth() throws SSDBException;
}
