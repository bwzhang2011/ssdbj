package com.lovver.ssdbj.util;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import com.lovver.ssdbj.core.impl.SSDBConnection;
import com.lovver.ssdbj.pool.SSDBPoolConnection;

public final class SSDBHelper {

	public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

	private SSDBHelper() {

	}

	public static List<byte[]> getByteArrayParams(List<String> params) {
		List<byte[]> bP = new ArrayList<byte[]>(params.size());

		for (String param : params) {
			bP.add(param.getBytes(DEFAULT_CHARSET));
		}

		return bP;
	}

	public static List<byte[]> getByteArrayParams(String... params) {
		List<byte[]> bP = new ArrayList<byte[]>(params.length);

		for (String param : params) {
			bP.add(param.getBytes(DEFAULT_CHARSET));
		}

		return bP;
	}

	public static void closeConnection(SSDBPoolConnection conn) {
		if (conn != null)
			conn.close();
	}

	public static void closeConnection(SSDBConnection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
			}
		}
	}
}
