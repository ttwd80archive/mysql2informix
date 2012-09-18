package com.twistlet.mysql2informix;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Hello world!
 * 
 */
public class App {
	public static void main(final String[] args) throws IOException {
		final ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:application-context.xml");
		final CreateService createService = applicationContext.getBean(CreateService.class);
		final List<String> list = createService.list();
		Collections.sort(list);
		final Map<String, Set<String>> map = createService.createDependencyMap(list);
		final List<String> sortedList = createService.sortByDependencies(map);
		final List<String> reverseSortedList = new ArrayList<>(sortedList);
		Collections.reverse(reverseSortedList);
		{
			final List<String> dropList = new ArrayList<>();
			for (final String item : reverseSortedList) {
				dropList.add("DROP TABLE " + item + ";");
			}
			FileUtils.writeLines(new File("drop.sql"), dropList);
		}
		{
			final List<String> createStatements = createService.createTables(sortedList);
			FileUtils.writeLines(new File("create.sql"), createStatements);
		}
		{
			final List<String> insertStatements = createService.insertStatements(sortedList);
			FileUtils.writeLines(new File("insert.sql"), insertStatements);
		}
	}
}
