package com.lovver.ssdbj.core.impl;

import com.lovver.ssdbj.core.BaseResultSet;

public class SSDBResultSet<T> implements BaseResultSet<T> {
	private final String status;
	private final T result;
	private final Exception e;

	public SSDBResultSet(String status, T result, Exception e) {
		super();
		this.status = status;
		this.result = result;
		this.e = e;
	}

	public SSDBResultSet(String status, T result) {
		this(status, result, null);
	}

	public SSDBResultSet(String status, Exception e) {
		this(status, null, e);
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
