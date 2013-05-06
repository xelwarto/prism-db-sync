/************************************
 * ConfigHandler.java
 *
 * Used to handle the database connections and operations
 *
 * Written by: Ted Elwartowski
 ************************************/
package org.jdog.prism.handler;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.jdog.prism.util.Variables;

/**
 * 
 * @author Ted Elwartowski
 */
public class DataBaseHandler {

	private Connection conn = null;
	private String type = null;
	private String name = null;
	private String errorHead = null;
	private boolean transactional = false;

	public DataBaseHandler(String type, String name) {
		this.type = type;
		this.name = name;
		errorHead = "DataBaseHandler Error(" + getType() + ":" + getName()
				+ ") ";
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public void setTransactional(boolean transactional) throws Exception {
		this.transactional = transactional;
		if (this.transactional && conn != null) {
			conn.setAutoCommit(false);
		}
	}

	public boolean isTransactional() {
		return transactional;
	}

	public void getConnection(String[] properties) throws Exception {
		if (properties != null && properties.length == 4) {
			Properties dbProperty = new Properties();
			dbProperty.put("user", properties[2]);
			dbProperty.put("password", properties[3]);

			Class.forName(properties[0]).newInstance();
			conn = DriverManager.getConnection(properties[1], dbProperty);
			if (transactional) {
				conn.setAutoCommit(false);
			}
		} else {
			throw new Exception(errorHead
					+ "connection properties are null or incorrect");
		}
	}

	public List<String> getTables() throws Exception {
		List<String> tables = new ArrayList<String>();
		if (conn != null) {
			String[] types = { "TABLE" };
			DatabaseMetaData data = conn.getMetaData();
			ResultSet results = data.getTables(null, null, "%", types);
			while (results.next()) {
				String tableName = (String) results.getString("TABLE_NAME");
				tables.add(tableName);
			}
		}
		return tables;
	}

	public List<String> getColumnList(String table) throws Exception {
		List<String> columns = new ArrayList<String>();
		if (conn != null) {
			DatabaseMetaData data = conn.getMetaData();
			ResultSet results = data.getColumns(null, null, table, "%");
			while (results.next()) {
				String colName = (String) results.getString("COLUMN_NAME");
				columns.add(colName);
			}
		}
		return columns;
	}

	public void clearTable(String table) throws Exception {
		if (table != null && !table.equals("")) {
			if (conn != null) {
				String sql = "delete from " + table;
				Statement stm = conn.createStatement();
				stm.executeUpdate(sql);
				stm.close();
			} else {
				throw new Exception(errorHead + "null pointer exception (conn)");
			}
		}
	}

	public void loadTable(String table, List<Object[]> loadData)
			throws Exception {
		if (table != null && !table.equals("")) {
			if (loadData != null && !loadData.isEmpty()) {
				Iterator<Object[]> dataIt = loadData.iterator();
				while (dataIt.hasNext()) {
					Object[] data = (Object[]) dataIt.next();
					if (data != null && data.length > 0) {
						String sql = _buildInsert(table, data.length);
						execQuery(sql, data);
					}
				}
			}
		}
	}

	public List<Object[]> unloadTable(String table) throws Exception {
		if (table != null && !table.equals("")) {
			String sql = "select * from " + table;
			return execQuery(sql);
		}
		return null;
	}

	public int getRecordCount(String table) throws Exception {
		if (table != null && !table.equals("")) {
			String sql = "select count(*) from " + table;
			Statement stm = conn.createStatement();
			ResultSet res = stm.executeQuery(sql);
			if (res.next()) {
				int count = res.getInt(1);
				return count;
			}
		}
		return 0;
	}

	public void commit() throws Exception {
		if (conn != null && transactional) {
			conn.commit();
		}
	}

	public void rollBack() throws Exception {
		if (conn != null && transactional) {
			conn.rollback();
		}
	}

	public void execQuery(String sql, Object[] data) throws Exception {
		if (conn != null) {
			if (sql != null) {

				PreparedStatement pstm = conn.prepareStatement(sql);
				if (data != null && data.length > 0) {
					for (int i = 0; i < data.length; i++) {
						if (data[i] == null) {
							pstm.setNull(i + 1, Types.NULL);
						} else {
							String dataType = data[i].getClass().toString();
							if (dataType.contains("String")) {
								pstm.setString(i + 1, (String) data[i]);
							} else if (dataType.contains("Date")) {
								pstm.setDate(i + 1, (java.sql.Date) data[i]);
							} else {
								pstm.setObject(i + 1, data[i]);
							}
						}
					}
				}
				pstm.execute();
				pstm.close();
			} else {
				throw new Exception(errorHead + "null pointer exception (sql)");
			}
		} else {
			throw new Exception(errorHead + "null pointer exception (conn)");
		}
	}

	public List<Object[]> execQuery(String sql) throws Exception {
		if (conn != null) {
			if (sql != null) {
				List<Object[]> results = new ArrayList<Object[]>();
				Statement stm = conn.createStatement();
				ResultSet res = stm.executeQuery(sql);
				while (res.next()) {
					ResultSetMetaData md = res.getMetaData();
					int numCols = md.getColumnCount();
					if (numCols > 0) {
						Object[] data = new Object[numCols];
						for (int i = 1; i <= numCols; i++) {
							data[i - 1] = res.getObject(i);
						}
						results.add(data);
					}
				}
				res.close();
				stm.close();

				return results;
			} else {
				throw new Exception(errorHead + "null pointer exception (sql)");
			}
		} else {
			throw new Exception(errorHead + "null pointer exception (conn)");
		}
	}

	public void close() throws Exception {
		if (conn != null) {
			conn.close();
			conn = null;
		}
	}
	
	public void fKeyControl(String value) throws Exception {
		this.execQuery(Variables.FKEYCHK_SQL + value, new Object[0]);
	}

	private String _buildInsert(String table, int cols) {
		String sql = "insert into ";
		sql = sql.concat(table);
		sql = sql.concat(" values(");
		for (int i = 0; i < cols; i++) {
			if (i != 0) {
				sql = sql.concat(",");
			}
			sql = sql.concat("?");
		}
		sql = sql.concat(")");
		return sql;
	}
}
