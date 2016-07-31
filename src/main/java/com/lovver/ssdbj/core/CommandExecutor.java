package com.lovver.ssdbj.core;

import java.util.List;

import com.lovver.ssdbj.exception.SSDBException;

public interface CommandExecutor {

	BaseResultSet execute(String cmd, List<byte[]> params) throws SSDBException;

	boolean executeUpdate(String cmd, List<byte[]> params) throws SSDBException;

}
