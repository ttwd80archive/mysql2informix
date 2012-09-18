package com.twistlet.mysql2informix;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface CreateService {
	List<String> list();

	List<String> sortByDependencies(Map<String, Set<String>> map);

	Map<String, Set<String>> createDependencyMap(List<String> list);

	List<String> createTables(List<String> list);

	List<String> insertStatements(List<String> list);

}
