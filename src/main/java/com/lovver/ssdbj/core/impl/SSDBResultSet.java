package com.lovver.ssdbj.core.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.lovver.ssdbj.core.BaseResultSet;

public class SSDBResultSet<T> implements BaseResultSet<T> {
	public final List<byte[]> raw;
	private final String status;
	private final T result;
	private final Exception e;

	public SSDBResultSet(String status, List<byte[]> raw, T result, Exception e) {
		super();
		this.status = status;
		this.raw = raw;
		this.result = result;
		this.e = e;
	}

	public SSDBResultSet(String status, List<byte[]> raw, T result) {
		this(status, raw, result, null);
	}

	public SSDBResultSet(String status, Exception e) {
		this(status, null, null, e);
	}

	@Override
	public T getResult() {
		return result;
	}

	@Override
	public String getStatus() {
		return status;
	}

	public Exception getE() {
		return e;
	}

}
