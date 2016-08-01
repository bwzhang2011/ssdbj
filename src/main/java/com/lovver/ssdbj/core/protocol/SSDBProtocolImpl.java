package com.lovver.ssdbj.core.protocol;

import static com.lovver.ssdbj.util.SSDBHelper.getByteArrayParams;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lovver.ssdbj.core.BaseResultSet;
import com.lovver.ssdbj.core.CommandExecutor;
import com.lovver.ssdbj.core.MemoryStream;
import com.lovver.ssdbj.core.Protocol;
import com.lovver.ssdbj.core.SSDBCmd;
import com.lovver.ssdbj.core.Stream2ResultSet;
import com.lovver.ssdbj.core.impl.SSDBResultSet;
import com.lovver.ssdbj.exception.SSDBException;

import jodd.util.StringUtil;

public class SSDBProtocolImpl implements Protocol {
	private static final Logger LOGGER = LoggerFactory.getLogger(SSDBProtocolImpl.class);

	private String protocolName = "ssdb";
	private String protocolVersion = "1.0v";

	private MemoryStream input = new MemoryStream();
	private final OutputStream outputStream;
	private final InputStream inputStream;
	private Properties props;

	private static final Map<String, SSDBCmd> ssdbCmdValueMap = new HashMap<String, SSDBCmd>(16);

	static {
		for (SSDBCmd ssdbCmd : SSDBCmd.values()) {
			ssdbCmdValueMap.put(ssdbCmd.getCmd(), ssdbCmd);
		}
	}

	public SSDBProtocolImpl(OutputStream os, InputStream is, Properties infos) {
		this.outputStream = os;
		this.inputStream = is;
		this.props = infos;
	}

	public void sendCommand(String cmd, List<byte[]> params) throws SSDBException {
		MemoryStream buf = new MemoryStream(SEND_BUFFER);
		Integer len = cmd.length();
		buf.write(len.toString());
		buf.write('\n');
		buf.write(cmd);
		buf.write('\n');
		for (byte[] bs : params) {
			len = bs.length;
			buf.write(len.toString());
			buf.write('\n');
			buf.write(bs);
			buf.write('\n');
		}
		buf.write('\n');
		try {
			outputStream.write(buf.buf, buf.data, buf.size);
			outputStream.flush();
		} catch (Exception e) {
			LOGGER.error("SendCmd: {} fail: {}", cmd, e);
			throw new SSDBException(e);
		}
	}

	public List<byte[]> receive() throws SSDBException {
		try {
			input.nice();
			while (true) {
				List<byte[]> ret = parse();
				if (ret != null) {
					return ret;
				}
				byte[] bs = new byte[READ_BUFFER];
				int len = inputStream.read(bs);

				input.write(bs, 0, len);
			}
		} catch (Exception e) {
			LOGGER.error("Receive fail", e);
			throw new SSDBException(e);
		}
	}

	private List<byte[]> parse() {
		ArrayList<byte[]> list = new ArrayList<byte[]>();
		byte[] buf = input.buf;

		int idx = 0;
		while (true) {
			int pos = input.memchr('\n', idx);
			// System.out.println("pos: " + pos + " idx: " + idx);
			if (pos == -1) {
				break;
			}
			if (pos == idx || (pos == idx + 1 && buf[idx] == '\r')) {
				// ignore empty leading lines
				if (list.isEmpty()) {
					idx += 1; // if '\r', next time will skip '\n'
					continue;
				} else {
					input.decr(idx + 1);
					return list;
				}
			}
			String str = new String(buf, input.data + idx, pos - idx);
			int len = Integer.parseInt(str);
			idx = pos + 1;
			if (idx + len >= input.size) {
				break;
			}
			byte[] data = Arrays.copyOfRange(buf, input.data + idx, input.data + idx + len);
			// System.out.println("len: " + len + " data: " + data.length);
			idx += len + 1; // skip '\n'
			list.add(data);
		}
		return null;
	}

	@Override
	public String getProtocol() {
		return protocolName;
	}

	@Override
	public String getProtocolVersion() {
		return protocolVersion;
	}

	private static class StreamResultSetWrapper implements Stream2ResultSet {

		private final List<byte[]> result;

		private String status;

		private final SSDBCmd cmd;

		/**
		 * Indicates items' order
		 */
		private List<byte[]> keys = new ArrayList<byte[]>();

		/**
		 * key-value results
		 */
		private Map<byte[], byte[]> items = new LinkedHashMap<byte[], byte[]>();

		public StreamResultSetWrapper(List<byte[]> result, String cmd) {
			this.result = result;
			if (result.size() > 0) {
				status = new String(result.get(0));
			}
			this.cmd = ssdbCmdValueMap.get(cmd);
		}

		@Override
		public BaseResultSet execute() {
			try {
				if (cmd == null) {
					return null;
				}

				BaseResultSet baseResult = null;

				switch (cmd) {
				case GET:
					baseResult = getCmdSSDBResultSet();
					break;
				case SCAN:
					baseResult = scanCmdSSDBResultSet();
					break;
				case RSCAN:
					baseResult = rscanCmdSSDBResultSet();
					break;
				case INCR:
					baseResult = incrCmdSSDBResultSet();
					break;
				case HGET:
					baseResult = hgetCmdSSDBResultSet();
					break;
				case HSCAN:
					baseResult = hscanCmdSSDBResultSet();
					break;
				case HRSCAN:
					baseResult = hrscanCmdSSDBResultSet();
					break;
				case HINCR:
					baseResult = hincrCmdSSDBResultSet();
					break;
				case ZGET:
					baseResult = zgetCmdSSDBResultSet();
					break;
				case ZSCAN:
					baseResult = zscanCmdSSDBResultSet();
					break;
				case ZRSCAN:
					baseResult = zrscanCmdSSDBResultSet();
					break;
				case ZINCR:
					baseResult = zincrCmdSSDBResultSet();
					break;
				case MULTI_GET:
					baseResult = multiGetCmdSSDBResultSet();
					break;
				case MULTI_DEL:
					baseResult = multiDelCmdSSDBResultSet();
					break;
				case PING:
					baseResult = pingCmdSSDBResultSet();
					break;
				default:
					break;
				}

				return baseResult;

			} catch (Exception e) {
				LOGGER.error("Cmd:{} execute fail:{}", cmd, e);
				return new SSDBResultSet("error", e);
			}
		}

		private SSDBResultSet<byte[]> getCmdSSDBResultSet() throws Exception {
			if (result.size() != CHECK_SIZE) {
				if ("not_found".equals(status)) {
					return new SSDBResultSet<byte[]>(status, null, null);
				}
				throw new Exception("Invalid getcmd response");
			}

			return new SSDBResultSet<byte[]>(status, result.get(1));
		}

		private SSDBResultSet<Map<byte[], byte[]>> scanCmdSSDBResultSet() throws Exception {
			buildMap();

			return new SSDBResultSet<Map<byte[], byte[]>>(status, items);
		}

		private SSDBResultSet<Map<byte[], byte[]>> rscanCmdSSDBResultSet() throws Exception {
			return scanCmdSSDBResultSet();
		}

		private SSDBResultSet<Long> incrCmdSSDBResultSet() throws Exception {
			if (result.size() != CHECK_SIZE) {
				if ("not_found".equals(status)) {
					return new SSDBResultSet<Long>(status, null, null);
				}
				throw new Exception("Invalid incrcmd response");
			}

			long ret = Long.parseLong(new String(result.get(1)));

			return new SSDBResultSet<Long>(status, ret);
		}

		private SSDBResultSet<byte[]> hgetCmdSSDBResultSet() throws Exception {
			if (result.size() != CHECK_SIZE) {
				if ("not_found".equals(status)) {
					return new SSDBResultSet<byte[]>(status, null, null);
				}
				throw new Exception("Invalid hgetcmd response");
			}
			return new SSDBResultSet<byte[]>(status, result.get(1));
		}

		private SSDBResultSet<Map<byte[], byte[]>> hscanCmdSSDBResultSet() throws Exception {
			buildMap();

			return new SSDBResultSet<Map<byte[], byte[]>>(status, items);
		}

		private SSDBResultSet<Map<byte[], byte[]>> hrscanCmdSSDBResultSet() throws Exception {
			return hscanCmdSSDBResultSet();
		}

		private SSDBResultSet<Long> hincrCmdSSDBResultSet() throws Exception {
			if (result.size() != CHECK_SIZE) {
				if ("not_found".equals(status)) {
					return new SSDBResultSet<Long>(status, null, null);
				}
				throw new Exception("Invalid hincrcmd response");
			}
			long ret = Long.parseLong(new String(result.get(1)));

			return new SSDBResultSet<Long>(status, ret);
		}

		private SSDBResultSet<Long> zgetCmdSSDBResultSet() throws Exception {
			if (result.size() != CHECK_SIZE) {
				if ("not_found".equals(status)) {
					return new SSDBResultSet<Long>(status, null, null);
				}
				throw new Exception("Invalid zgetcmd response");
			}
			long ret = Long.parseLong(new String(result.get(1)));

			return new SSDBResultSet<Long>(status, ret);
		}

		private SSDBResultSet<Map<byte[], byte[]>> zscanCmdSSDBResultSet() throws Exception {
			buildMap();

			return new SSDBResultSet<Map<byte[], byte[]>>(status, items);
		}

		private SSDBResultSet<Map<byte[], byte[]>> zrscanCmdSSDBResultSet() throws Exception {
			return zscanCmdSSDBResultSet();
		}

		private SSDBResultSet<Long> zincrCmdSSDBResultSet() throws Exception {
			if (result.size() != CHECK_SIZE) {
				if ("not_found".equals(status)) {
					// return new SSDBResultSet<byte[]>(status, result, null);
					return new SSDBResultSet<Long>(status, null, null);
				}
				throw new Exception("Invalid zincrcmd response");
			}
			long ret = Long.parseLong(new String(result.get(1)));

			return new SSDBResultSet<Long>(status, ret);
		}

		private SSDBResultSet<Map<byte[], byte[]>> multiGetCmdSSDBResultSet() throws Exception {
			buildMap();

			return new SSDBResultSet<Map<byte[], byte[]>>(status, items);
		}

		private SSDBResultSet<Map<byte[], byte[]>> multiDelCmdSSDBResultSet() throws Exception {
			return multiGetCmdSSDBResultSet();
		}

		private SSDBResultSet<Map<byte[], byte[]>> pingCmdSSDBResultSet() throws Exception {
			return new SSDBResultSet<Map<byte[], byte[]>>(status, null, null);
		}

		private void buildMap() {
			for (int i = 1; i + 1 < result.size(); i += 2) {
				byte[] k = result.get(i);
				byte[] v = result.get(i + 1);
				keys.add(k);
				items.put(k, v);
			}
		}

	}

	private static class CommandExecutorWrapper implements CommandExecutor {
		private final SSDBProtocolImpl ssdbProtocolImpl;

		public CommandExecutorWrapper(SSDBProtocolImpl ssdbProtocolImpl) {
			this.ssdbProtocolImpl = ssdbProtocolImpl;
		}

		@Override
		public BaseResultSet execute(String cmd, List<byte[]> params) throws SSDBException {
			List<byte[]> result = getCommandResult(cmd, params);

			String cmd_t = cmd.toLowerCase();

			return new StreamResultSetWrapper(result, cmd_t).execute();
		}

		@Override
		public boolean executeUpdate(String cmd, List<byte[]> params) throws SSDBException {
			try {
				List<byte[]> result = getCommandResult(cmd, params);
				return "ok".equals(new String(result.get(0)));
			} catch (Exception e) {
				LOGGER.error("Cmd:{} executeUpdate fail:{}", cmd, e);
				return false;
			}
		}

		private List<byte[]> getCommandResult(String cmd, List<byte[]> params) throws SSDBException {
			ssdbProtocolImpl.sendCommand(cmd, params);
			List<byte[]> result = ssdbProtocolImpl.receive();
			return result;
		}

	}

	@Override
	public CommandExecutor getCommandExecutor() {
		return new CommandExecutorWrapper(this);
	}

	@Override
	public void auth() {
		String sauth = props.getProperty("password");

		if (StringUtil.isBlank(sauth)) {
			return;
		}

		try {
			List<byte[]> auth = getByteArrayParams(sauth);
			sendCommand("auth", auth);
			List<byte[]> authResult = receive();
			if (!"ok".equals(new String(authResult.get(0)))) {
				LOGGER.error("auth fail:{}", sauth);
				throw new RuntimeException("auth failed");
			}
		} catch (SSDBException e) {
			LOGGER.error("auth:{} fail:{}", sauth, e);
			throw new RuntimeException("auth failed");
		}
	}
}
