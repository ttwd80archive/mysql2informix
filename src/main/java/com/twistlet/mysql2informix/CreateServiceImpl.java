package com.twistlet.mysql2informix;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

public class CreateServiceImpl implements CreateService {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private final JdbcTemplate jdbcTemplate;

	public CreateServiceImpl(final JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public List<String> list() {
		return jdbcTemplate.execute(new TableListConnectionCallback());
	}

	@Override
	public List<String> sortByDependencies(final Map<String, Set<String>> map) {
		return jdbcTemplate.execute(new TableCreationOrderConnectionCallback(map));
	}

	@Override
	public Map<String, Set<String>> createDependencyMap(final List<String> list) {
		return jdbcTemplate.execute(new TableDependencyMapConnectionCallback(list));
	}

	@Override
	public List<String> createTables(final List<String> list) {
		return jdbcTemplate.execute(new TableCreationStatementConnectionCallback(list));
	}

	@Override
	public List<String> insertStatements(final List<String> list) {
		final List<String> bigList = new ArrayList<>();
		for (final String table : list) {
			final ResultSetExtractor<List<String>> rse = new DummyResultSetExtractor(table);
			final String sql = "SELECT * from " + table;
			final List<String> insertStatements = jdbcTemplate.query(sql, rse);
			bigList.addAll(insertStatements);
		}
		return bigList;
	}

	private class DummyResultSetExtractor implements ResultSetExtractor<List<String>> {

		private final String table;

		public DummyResultSetExtractor(final String table) {
			this.table = table;
		}

		@Override
		public List<String> extractData(final ResultSet rs) throws SQLException, DataAccessException {
			final List<String> list = new ArrayList<>();
			final ResultSetMetaData resultSetMetaData = rs.getMetaData();
			final int count = resultSetMetaData.getColumnCount();
			final Map<String, Integer> map = new LinkedHashMap<>();
			for (int i = 1; i <= count; i++) {
				final String name = resultSetMetaData.getColumnName(i);
				final int columnType = resultSetMetaData.getColumnType(i);
				map.put(name, columnType);
			}
			while (rs.next()) {
				final String field = StringUtils.join(map.keySet().toArray(), ", ");
				final String values = StringUtils.join(toValues(rs, map), ", ");
				final String line = "INSERT INTO " + table + " (" + field + ") VALUES (" + values + ");";
				list.add(line);
				System.out.println(line);
			}

			// TODO Auto-generated method stub
			return list;
		}

		private List<String> toValues(final ResultSet rs, final Map<String, Integer> map) throws SQLException {
			final List<String> list = new ArrayList<>();
			final Set<String> set = map.keySet();
			for (final String key : set) {
				final Integer columnType = map.get(key);
				final String value = toValue(rs, key, columnType);
				list.add(value);
			}
			return list;
		}

		private String toValue(final ResultSet rs, final String key, final int columnType) throws SQLException {
			final Object value = rs.getObject(key);
			if (value == null) {
				return "null";
			}
			switch (columnType) {
			case Types.CHAR:
				return toString(rs, key);
			case Types.VARCHAR:
				return toString(rs, key);
			case Types.INTEGER:
				return toInteger(rs, key);
			case Types.DECIMAL:
				return toDecimal(rs, key);
			case Types.DATE:
				return toTimestamp(rs, key);
			case Types.TIMESTAMP:
				return toDate(rs, key);
			case Types.BIT:
				return toBoolean(rs, key);

			default:
				System.err.println("don't know how to handle " + key + " - " + columnType);
				System.exit(0);
				return "";
			}
		}

		private String toBoolean(final ResultSet rs, final String key) throws SQLException {
			return "'" + BooleanUtils.toString(rs.getBoolean(key), "T", "F") + "'";
		}

		private String toString(final ResultSet rs, final String key) throws SQLException {
			final String value = rs.getString(key);
			try {
				final byte[] bytes = value.getBytes("US-ASCII");
				final String text = new String(bytes);
				return "'" + text.replace("\'", "\'\'") + "'";

			} catch (final UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		private String toInteger(final ResultSet rs, final String key) throws SQLException {
			return Integer.toString(rs.getInt(key));

		}

		private String toDecimal(final ResultSet rs, final String key) throws SQLException {
			return rs.getBigDecimal(key).toPlainString();
		}

		private String toDate(final ResultSet rs, final String key) throws SQLException {
			final Date date = rs.getDate(key);
			final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-M-d");
			return "DATETIME (" + sdf.format(date) + ") YEAR TO DAY";
		}

		private String toTimestamp(final ResultSet rs, final String key) throws SQLException {
			final Date date = rs.getDate(key);
			final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-M-d HH:mm:ss.SS");
			return "DATETIME (" + sdf.format(date) + ") YEAR TO FRACTION";
		}

	};

}
