/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jdog.prism.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;

/**
 * 
 * @author ted
 */
public class QueryHandler {

	private DataBaseHandler srcDatabase = null;
	private DataBaseHandler destDatabase = null;
	private LogHandler log = null;
	private boolean hasError = false;
	private List<HashMap<String, Object>> rollBacks = new ArrayList<HashMap<String, Object>>();

	public QueryHandler(LogHandler log) throws Exception {
		if (log != null) {
			this.log = log;
		} else {
			throw new Exception(
					"QueryHandler Error: null pointer exception (log)");
		}
	}

	public void setDest(DataBaseHandler destDatabase) throws Exception {
		if (destDatabase != null) {
			this.destDatabase = destDatabase;
		} else {
			throw new Exception(
					"QueryHandler Error: null pointer exception (destDatabase)");
		}
	}

	public void setSrc(DataBaseHandler srcDatabase) throws Exception {
		if (srcDatabase != null) {
			this.srcDatabase = srcDatabase;
		} else {
			throw new Exception(
					"QueryHandler Error: null pointer exception (srcDatabase)");
		}
	}

	public boolean hasError() {
		return hasError;
	}

	public void commit() {
		if (!(rollBacks.size() > 0)) {
			try {
				if (srcDatabase.isTransactional()) {
					log.verbose("QueryHandler: Commiting source database changes");
					srcDatabase.commit();
				}

				if (destDatabase.isTransactional()) {
					log.verbose("QueryHandler: Commiting destination database changes");
					destDatabase.commit();
				}
			} catch (Exception e) {
				hasError = true;
				log.log("QueryHandler Error: " + e.toString());
			}
		}
	}

	public void rollBack() {
		if (rollBacks.size() > 0) {
			log.log("QueryHandler: Attempting to roll back changes");
			try {
				for (int i = rollBacks.size() - 1; i >= 0; i--) {
					HashMap<String, Object> rollBack = (HashMap<String, Object>) rollBacks
							.get(i);
					if (rollBack != null) {
						String query = (String) rollBack.get("QUERY");
						String dataBase = (String) rollBack.get("DB");

						if (query != null && dataBase != null) {
							if (dataBase.equalsIgnoreCase("src")) {
								log.debug("QueryHandler: Running query on source database: "
										+ query);
								srcDatabase.execQuery(query, null);
							} else if (dataBase.equalsIgnoreCase("dest")) {
								log.debug("QueryHandler: Running query on destination database: "
										+ query);
								destDatabase.execQuery(query, null);
							}
						} else {
							log.log("QueryHandler Error: Rollback failed - query or database not found");
							break;
						}
					} else {
						log.log("QueryHandler Error: Rollback failed - rollback object not found");
						break;
					}
				}
			} catch (Exception e) {
				log.log("QueryHandler Error: " + e.toString());
			}
		} else {
			try {
				if (destDatabase.isTransactional()) {
					log.log("QueryHandler: Attempting to roll back destination database changes");
					destDatabase.rollBack();
				}
				if (srcDatabase.isTransactional()) {
					log.log("QueryHandler: Attempting to roll back source database changes");
					srcDatabase.rollBack();
				}
			} catch (Exception e) {
				hasError = true;
				log.log("QueryHandler Error: " + e.toString());
			}
		}
	}

	public void syncQueries(HashMap<String, Object> query) {
		hasError = false;
		if (query != null) {
			String queryName = (String) query.get("NAME");
			if (queryName != null) {
				log.debug("QueryHandler: Running query: " + queryName);

				String sourceQuery = null;
				String destQuery = null;
				try {
					/*
					 * FIX HERE
					List<HashMap<String, Object>> rollBack = query.get("RLBK");
					
					if (rollBack != null && rollBack.size() > 0) {
						log.debug("QueryHandler: Adding query rollbacks: "
								+ rollBack.size());
						Iterator<HashMap<String, Object>> it = rollBack.iterator();
						while (it.hasNext()) {
							rollBacks.add((HashMap<String, Object>) it.next());
						}
					}
					*/
					
					List<Object[]> srcDataList = null;
					sourceQuery = (String) query.get("SRC");
					if (sourceQuery != null) {
						log.verbose("QueryHandler: Running source query: "
								+ sourceQuery);
						srcDataList = srcDatabase.execQuery(sourceQuery);

						if (srcDataList != null && srcDataList.size() > 0) {
							log.verbose("QueryHandler: Data set records found: "
									+ srcDataList.size());
						}
					}

					destQuery = (String) query.get("DST");
					if (destQuery != null) {
						log.verbose("QueryHandler: Running destination query: "
								+ destQuery);
						if (srcDataList != null && srcDataList.size() > 0) {
							Iterator<Object[]> dataIt = srcDataList.iterator();
							while (dataIt.hasNext()) {
								Object[] data = (Object[]) dataIt.next();
								if (data != null && data.length > 0) {
									destDatabase.execQuery(destQuery, data);
								}
							}
						} else {
							destDatabase.execQuery(destQuery, null);
						}
					}

					String checkSum = (String) query.get("CKSM");
					// String checkSumLevel = (String) query.get("CKSMLVL");
					if (checkSum != null && !checkSum.equals("")) {
						log.debug("QueryHandler: Running checksum query: "
								+ checkSum);
						if (srcDataList != null && srcDataList.size() > 0) {
							List<Object[]> chkDataList = destDatabase
									.execQuery(checkSum);
							log.verbose("QueryHandler: Check sum records found: "
									+ chkDataList.size());

							log.debug("QueryHandler: Validating destination record count with source");
							if (srcDataList.size() != chkDataList.size()) {
								throw new Exception(
										"Checksum failed - number of records are not equal");
							}

							/*
							 * TODO - Fix the HIGH level check sum
							 */
							// if (checkSumLevel != null &&
							// checkSumLevel.equalsIgnoreCase("high")) {
							// log.debug("QueryHandler: Validating destination data with source - Check sum level = High");
							// if (!srcDataList.equals(chkDataList)) {
							// throw new
							// Exception("Checksum failed - check sum data does not equal source data");
							// }
							// }
						} else {
							throw new Exception(
									"Checksum failed - source data set null or empty");
						}
					}
				} catch (Exception e) {
					hasError = true;
					log.log("QueryHandler Error: " + e.toString());
				}
			} else {
				hasError = true;
				log.log("QueryHandler Error: null pointer exception (queryName)");
			}

		} else {
			hasError = true;
			log.log("QueryHandler Error: null pointer exception (query)");
		}
	}
}
