package com.twistlet.mysql2informix;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

public class TableListConnectionCallback implements
		ConnectionCallback<List<String>> {

	@Override
	public List<String> doInConnection(final Connection con)
			throws SQLException, DataAccessException {
		final List<String> result = new ArrayList<String>();
		final DatabaseMetaData metadata = con.getMetaData();
		final ResultSet rs = metadata.getTables(null, null, null,
				new String[] { "TABLE" });
		while (rs.next()) {
			final String value = rs.getString("TABLE_NAME");
			result.add(value);
		}
		return result;
	}
}