package com.twistlet.mysql2informix;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

public class TableDependencyMapConnectionCallback implements
		ConnectionCallback<Map<String, Set<String>>> {
	protected Logger logger = LoggerFactory.getLogger(getClass());

	private final List<String> list;

	public TableDependencyMapConnectionCallback(final List<String> list) {
		this.list = list;
	}

	@Override
	public Map<String, Set<String>> doInConnection(final Connection con)
			throws SQLException, DataAccessException {
		final DatabaseMetaData metadata = con.getMetaData();
		final Map<String, Set<String>> map = new LinkedHashMap<String, Set<String>>();
		final ArrayList<String> sortedList = new ArrayList<String>(list);
		for (final String item : sortedList) {
			map.put(item, new LinkedHashSet<String>());
		}
		for (final String item : sortedList) {
			logger.debug("Processing {}", item);
			final ResultSet rs = metadata.getExportedKeys(null, null, item);
			while (rs.next()) {
				final String table = rs.getString("FKTABLE_NAME");
				if (table == null) {
					continue;
				}
				logger.debug("{} depends on {}", table, item);
				final Set<String> set = map.get(table);
				set.add(item);
			}
			rs.close();
		}
		return map;
	}
}
