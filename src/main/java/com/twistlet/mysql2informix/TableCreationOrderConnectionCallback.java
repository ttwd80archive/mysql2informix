package com.twistlet.mysql2informix;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

public class TableCreationOrderConnectionCallback implements
		ConnectionCallback<List<String>> {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	private final Map<String, Set<String>> map;

	public TableCreationOrderConnectionCallback(
			final Map<String, Set<String>> map) {
		this.map = map;
	}

	@Override
	public List<String> doInConnection(final Connection con)
			throws SQLException, DataAccessException {
		logger.debug("CREATION ORDER");
		final List<String> list = new ArrayList<String>();
		final List<String> baseList = new ArrayList<String>(map.keySet());
		Collections.sort(baseList);
		while (!baseList.isEmpty()) {
			final List<String> itemsToLoop = new ArrayList<String>(baseList);
			for (final String item : itemsToLoop) {
				if (isOkToAddItemToList(item, list)) {
					logger.debug("ADDING {}", item);
					list.add(item);
					baseList.remove(item);
				} else {
					logger.debug("NOT ADDING {}", item);
				}
			}
		}
		return list;
	}

	private boolean isOkToAddItemToList(final String item,
			final List<String> list) {
		final Set<String> set = map.get(item);
		for (final String table : set) {
			if (list.contains(table)) {
				logger.debug("{} depends on {}. All good.", item, table);
			} else {
				if (table.equals(item)) {
					logger.debug("{} is self dependent. All good.", item, table);
				} else {
					logger.debug("{} depends on {}. Not good.", item, table);
					return false;
				}
			}

		}
		return true;
	}
}
