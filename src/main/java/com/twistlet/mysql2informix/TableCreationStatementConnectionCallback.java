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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.RowMapper;

public class TableCreationStatementConnectionCallback implements ConnectionCallback<List<String>> {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	private final List<String> list;

	public TableCreationStatementConnectionCallback(final List<String> list) {
		this.list = list;
	}

	@Override
	public List<String> doInConnection(final Connection con) throws SQLException, DataAccessException {
		final List<String> statements = new ArrayList<>();
		final DatabaseMetaData metaData = con.getMetaData();
		for (final String table : list) {
			String statement = "CREATE TABLE " + table + " (";
			statement = statement + "\n";
			{
				final List<String> createLines = new ArrayList<>();
				createColumns(metaData, table, createLines);
				createPrimaryKey(metaData, table, createLines);
				createForeignKeys(metaData, table, createLines);
				statement = statement + StringUtils.join(createLines.toArray(), ",\n");
			}
			statement = statement + "\n";
			statement = statement + ");\n";
			statement = statement + StringUtils.join(createIndexesAndConstraints(metaData, table).toArray(), "\n");
			statements.add(statement);

		}
		return statements;
	}

	private void createForeignKeys(final DatabaseMetaData metaData, final String table, final List<String> createLines)
			throws SQLException {
		final ResultSet rs = metaData.getImportedKeys(null, null, table);
		while (rs.next()) {
			final short keySequence = rs.getShort("KEY_SEQ");
			if (keySequence > 1) {
				throw new RuntimeException("table " + table + " has a composite foreign key");
			}
			final String fkColumnName = rs.getString("FKCOLUMN_NAME");
			final String pkColumnName = rs.getString("PKCOLUMN_NAME");
			final String pkTableName = rs.getString("PKTABLE_NAME");
			final String line = "\t" + "FOREIGN KEY (" + fkColumnName + ") REFERENCES " + pkTableName + "(" + pkColumnName + ")";
			createLines.add(line);
		}
		rs.close();
	}

	private class CreateIndexesAndConstraintsRowMapper implements RowMapper<Object> {

		private final Map<String, Set<String>> uniqueIndexAndConstraintsMap = new LinkedHashMap<>();
		private final Map<String, Set<String>> nonUniqueIndexMap = new LinkedHashMap<>();
		private final Set<String> primaryKeyFields = new LinkedHashSet<>();

		@Override
		public Object mapRow(final ResultSet rs, final int rowNum) throws SQLException {
			final String indexName = rs.getString("INDEX_NAME");
			if ("PRIMARY".equals(indexName)) {
				final String columnName = rs.getString("COLUMN_NAME");
				primaryKeyFields.add(columnName);
				return null;
			}
			final Map<String, Set<String>> map;
			if (rs.getBoolean("NON_UNIQUE")) {
				map = nonUniqueIndexMap;
			} else {
				map = uniqueIndexAndConstraintsMap;
			}
			Set<String> set;
			if ((set = map.get(indexName)) == null) {
				set = new LinkedHashSet<>();
				map.put(indexName, set);
			}
			final short position = rs.getShort("ORDINAL_POSITION");
			if (position != set.size() + 1) {
				throw new NullPointerException();
			}
			final String columnName = rs.getString("COLUMN_NAME");
			set.add(columnName);
			return null;
		}

		public Map<String, Set<String>> getUniqueIndexAndConstraintsMap() {
			return uniqueIndexAndConstraintsMap;
		}

		public Map<String, Set<String>> getNonUniqueIndexMap() {
			return nonUniqueIndexMap;
		}

		private void removeIndexedPrimaryKey(final Map<String, Set<String>> map) {
			final Set<String> set = new LinkedHashSet<>(map.keySet());
			for (final String key : set) {
				final Set<String> values = map.get(key);
				final List<String> indexList = new ArrayList<>(values);
				if (CollectionUtils.isEqualCollection(primaryKeyFields, indexList)) {
					map.remove(key);
				}
			}
		}

		public void removeIndexedPrimaryKey() {
			removeIndexedPrimaryKey(uniqueIndexAndConstraintsMap);
			removeIndexedPrimaryKey(nonUniqueIndexMap);
		}

	}

	private List<String> createIndexesAndConstraints(final DatabaseMetaData metaData, final String table) throws SQLException {
		final List<String> list = new ArrayList<>();
		final ResultSet rs = metaData.getIndexInfo(null, null, table, false, false);
		int rowNum = 0;
		final CreateIndexesAndConstraintsRowMapper indexRowMapper = new CreateIndexesAndConstraintsRowMapper();
		while (rs.next()) {
			indexRowMapper.mapRow(rs, rowNum);
			rowNum++;
		}
		indexRowMapper.removeIndexedPrimaryKey();
		rs.close();
		final Map<String, Set<String>> uniqueIndexAndConstraintsMap = indexRowMapper.getUniqueIndexAndConstraintsMap();
		final Map<String, Set<String>> nonUniqueIndexMap = indexRowMapper.getNonUniqueIndexMap();
		final Map<String, Set<String>> uniqueIndexMap = new LinkedHashMap<>(uniqueIndexAndConstraintsMap);
		filterOutForeignKeyIndexes(nonUniqueIndexMap, metaData, table);
		filterOutForeignKeyIndexes(uniqueIndexMap, metaData, table);
		int count = 0;
		final Map<String, Set<String>> uniqueConstraintMap = subtract(uniqueIndexAndConstraintsMap, uniqueIndexMap);
		final List<String> listNonUniqueIndex = createIndex(table, nonUniqueIndexMap, false, count);
		list.addAll(listNonUniqueIndex);
		count = list.size();
		final List<String> listUniqueIndex = createIndex(table, uniqueIndexMap, true, count);
		list.addAll(listUniqueIndex);
		count = list.size();
		final List<String> listUniqueConstraint = createUniqueConstraint(table, uniqueConstraintMap);
		list.addAll(listUniqueConstraint);
		count = list.size();
		return list;
	}

	private List<String> createUniqueConstraint(final String table, final Map<String, Set<String>> map) {
		final List<String> list = new ArrayList<>();
		final Set<String> keys = map.keySet();
		for (final String key : keys) {
			final Set<String> set = map.get(key);
			final String columns = StringUtils.join(set.toArray(), ", ");
			final String sql = "ALTER TABLE " + table + " ADD CONSTRAINT UNIQUE (" + columns + ");";
			list.add(sql);
		}
		return list;
	}

	private Map<String, Set<String>> subtract(final Map<String, Set<String>> big, final Map<String, Set<String>> small) {
		final Map<String, Set<String>> result = new LinkedHashMap<>();
		final Set<String> set = big.keySet();
		for (final String key : set) {
			final Set<String> value = small.get(key);
			if (value == null) {
				result.put(key, big.get(key));
			}
		}
		return result;
	}

	private void filterOutForeignKeyIndexes(final Map<String, Set<String>> map, final DatabaseMetaData metaData, final String table)
			throws SQLException {
		final Set<String> keySet = new LinkedHashSet<>(map.keySet());
		for (final String key : keySet) {
			final Set<String> set = map.get(key);
			if (set.size() == 1) {
				final String column = new ArrayList<>(set).get(0);
				final ResultSet rs = metaData.getImportedKeys(null, null, table);
				while (rs.next()) {
					final String columnName = rs.getString("FKCOLUMN_NAME");
					if (StringUtils.equals(columnName, column)) {
						map.remove(key);
						break;
					}
				}
				rs.close();
			}
		}
	}

	private List<String> createIndex(final String table, final Map<String, Set<String>> map, final boolean unique, final int count) {
		final String uniquePortion = (unique == true ? " UNIQUE" : "");
		final List<String> list = new ArrayList<>();
		final Set<String> set = map.keySet();
		int line = 0;
		for (final String key : set) {
			line++;
			final Set<String> value = map.get(key);
			final String columns = StringUtils.join(value.toArray(), ", ");
			final String indexName = "idx_" + table + "_" + (count + line);
			final String sql = "CREATE" + uniquePortion + " " + "INDEX" + " " + indexName + " ON " + table + "(" + columns + ");";
			list.add(sql);
		}
		return list;
	}

	private int findLineForColumnName(final List<String> lines, final String columnName) {
		int index = 0;
		int found = -1;
		for (final String line : lines) {
			if (line.matches(".*\\b" + columnName + "\\b.*")) {
				if (found == -1) {
					found = index;
				} else {
					throw new RuntimeException();
				}
				index++;
			}
		}
		return found;
	}

	private void createPrimaryKey(final DatabaseMetaData metaData, final String table, final List<String> createLines)
			throws SQLException {
		final ResultSet rs = metaData.getPrimaryKeys(null, null, table);
		int index = 0;
		while (rs.next()) {
			if (index == 0) {
				final String columnName = rs.getString("COLUMN_NAME");
				final int lineIndex = findLineForColumnName(createLines, columnName);
				if (lineIndex >= 0) {
					String line = createLines.get(lineIndex);
					line = line + " PRIMARY KEY";
					createLines.set(lineIndex, line);
				}
			}
			index++;
		}
		rs.close();
		if (index != 1) {
			throw new RuntimeException("Primary key  Column count for table " + table + " = " + index);
		}

	}

	private void createColumns(final DatabaseMetaData metaData, final String table, final List<String> createLines)
			throws SQLException {
		final ResultSet rs = metaData.getColumns(null, null, table, null);
		final Map<String, String> typeMap = createTypeMap();
		final Map<String, Integer> typeLengthMap = createTypeLengthMap();
		while (rs.next()) {
			final String columnName = rs.getString("COLUMN_NAME");
			final String autoIncrement = rs.getString("IS_AUTOINCREMENT");
			final String columnDef = "\t" + columnName;
			final String columnType;
			if ("YES".equals(autoIncrement)) {
				columnType = "SERIAL";
			} else {
				columnType = createColumnType(rs, typeMap, typeLengthMap, table, columnName);
			}
			final String nullPortion = createNullPortion(rs);
			final String columnTypeSpace = (StringUtils.trimToNull(nullPortion) == null ? "" : " ");
			createLines.add(columnDef + " " + columnType + columnTypeSpace + nullPortion);
		}
		rs.close();
	}

	private String createColumnType(final ResultSet rs, final Map<String, String> typeMap,
			final Map<String, Integer> typeLengthMap, final String table, final String columnName) throws SQLException {
		String outputColumnType;
		final String columnType = rs.getString("TYPE_NAME");
		{
			outputColumnType = typeMap.get(columnType);
			if (outputColumnType == null) {
				throw new RuntimeException(columnType + " for " + table + "." + columnName + " is not known yet");
			}
		}
		String columnSizeSpecification;
		{
			final int dataTypeLength = typeLengthMap.get(columnType);
			switch (dataTypeLength) {
			case 1: {
				final int columnSize = rs.getInt("COLUMN_SIZE");
				if (columnSize <= 0) {
					throw new RuntimeException();
				}
				if (columnSize > 20 && outputColumnType.equals("DECIMAL")) {
					throw new RuntimeException("WTF");
				}
				columnSizeSpecification = "(" + columnSize + ")";
				if (columnSize > 255 && outputColumnType.equals("VARCHAR")) {
					outputColumnType = "LVARCHAR";
					columnSizeSpecification = "(" + columnSize + ")";
				}
			}
				break;
			case 2: {
				final int columnSize = rs.getInt("COLUMN_SIZE");
				final int decimalDigits = rs.getInt("DECIMAL_DIGITS");
				if (columnSize <= 0) {
					throw new RuntimeException();
				}
				columnSizeSpecification = "(" + columnSize + "," + decimalDigits + ")";
			}
				break;
			default:
				columnSizeSpecification = StringUtils.EMPTY;

			}
		}
		return outputColumnType + columnSizeSpecification;
	}

	private String createNullPortion(final ResultSet rs) throws SQLException {
		String nullPortion;
		final int nullable = rs.getInt("NULLABLE");
		if (DatabaseMetaData.columnNoNulls == nullable) {
			nullPortion = "NOT NULL";
		} else if (DatabaseMetaData.columnNullable == nullable) {
			nullPortion = StringUtils.EMPTY;
		} else {
			throw new NullPointerException();
		}
		return nullPortion;

	}

	private Map<String, String> createTypeMap() {
		final Map<String, String> map = new LinkedHashMap<>();
		map.put("INT", "INT");
		map.put("VARCHAR", "VARCHAR");
		map.put("CHAR", "CHAR");
		map.put("DECIMAL", "DECIMAL");
		map.put("DATE", "DATE");
		map.put("LONGTEXT", "CLOB");
		map.put("TIMESTAMP", "DATETIME YEAR to FRACTION");
		map.put("DATETIME", "DATETIME YEAR to FRACTION");
		map.put("BLOB", "BYTE");
		map.put("BIT", "BOOLEAN");
		return map;
	}

	private Map<String, Integer> createTypeLengthMap() {
		final Map<String, Integer> map = new LinkedHashMap<>();
		map.put("INT", 0);
		map.put("VARCHAR", 1);
		map.put("CHAR", 1);
		map.put("DECIMAL", 2);
		map.put("DATE", 0);
		map.put("INT UNSIGNED", 0);
		map.put("LONGTEXT", 0);
		map.put("BLOB", 0);
		map.put("BIT", 0);
		map.put("DATETIME", 0);
		map.put("TIMESTAMP", 0);
		return map;
	}
}
