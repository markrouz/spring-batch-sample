package com.example.demo.listener;

import com.example.demo.model.ItemRow;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class FileImportListener extends JobExecutionListenerSupport {

    private final JdbcTemplate jdbcTemplate;

    public FileImportListener(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("data in table before file has been loaded");
        logAll();
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("file is loaded to database");
            logAll();
        }
    }

    private void logAll() {
        jdbcTemplate.query("SELECT last_name, first_name, date FROM item_rows",
                (rs, row) -> new ItemRow(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3))
        ).forEach(r -> log.info(r.toString()));
    }

}
