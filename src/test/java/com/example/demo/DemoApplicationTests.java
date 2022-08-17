package com.example.demo;

import com.example.demo.model.ItemRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

@SpringBootTest
class DemoApplicationTests {

	@Autowired
	JdbcTemplate jdbcTemplate;

	@Test
	void contextLoads() {
	}

	@Test
	void testThatEntriesPresentInDatabase() {
		List<ItemRow> itemRows = jdbcTemplate.query("SELECT last_name, first_name, date FROM item_rows",
				(rs, row) -> new ItemRow(
						rs.getString(1),
						rs.getString(2),
						rs.getString(3))
		);
		Assertions.assertEquals(202, itemRows.size());
	}

}
