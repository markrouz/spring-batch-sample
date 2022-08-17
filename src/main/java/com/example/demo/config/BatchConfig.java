package com.example.demo.config;

import com.example.demo.model.ItemRow;
import com.example.demo.listener.FileImportListener;
import com.example.demo.zip.ZipExtractor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.FileSystemUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;

@Configuration
@EnableBatchProcessing
@RequiredArgsConstructor
@Slf4j
public class BatchConfig {

    final ResourcePatternResolver resourcePatternResolver;

    @Bean
    public Job importFileJob(JobBuilderFactory jobBuilderFactory,
                             Step loadResourcesStep,
                             Step deleteUnzippedFilesStep,
                             FileImportListener reportImportListener) {
        return jobBuilderFactory.get("import-file-job")
                .listener(reportImportListener)
                .start(loadResourcesStep)
                .next(deleteUnzippedFilesStep)
                .build();
    }


    @Bean
    public Step loadResourcesStep(StepBuilderFactory stepBuilderFactory,
                                  MultiResourcePartitioner multiResourcePartitioner,
                                  Step loadCsvStep) throws UnexpectedInputException, ParseException {
        return stepBuilderFactory.get("load-resources")
                .partitioner("loadCsvStep", multiResourcePartitioner)
                .step(loadCsvStep)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public MultiResourcePartitioner multiResourcePartitioner() {
        var multiResourcePartitioner = new MultiResourcePartitioner();

        Resource[] resources;
        try {
            ZipExtractor.extractDataZip();
            resources = resourcePatternResolver.getResources("file:" + ZipExtractor.CSV_FILE_DESTINATION_PATH + "/*.csv");
        } catch (IOException e) {
            throw new RuntimeException("I/O problems when resolving"
                    + " the input file pattern.", e);
        }
        multiResourcePartitioner.setResources(resources);
        return multiResourcePartitioner;
    }


    @Bean
    public Step loadCsvStep(StepBuilderFactory stepBuilderFactory,
                            FlatFileItemReader<ItemRow> csvReader,
                            JdbcBatchItemWriter<ItemRow> dbWriter) {
        return stepBuilderFactory.get("load-csv-file")
                .<ItemRow, ItemRow>chunk(1)
                .reader(csvReader)
                .writer(dbWriter)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<ItemRow> csvReader(@Value("#{stepExecutionContext[fileName]}") String filename) {
        return new FlatFileItemReaderBuilder<ItemRow>().name("csv-reader")
                .resource(resourcePatternResolver.getResource(filename))
                .targetType(ItemRow.class)
                .delimited()
                .delimiter(",")
                .names("firstName", "lastName", "date")
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<ItemRow> dbWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<ItemRow>()
                .dataSource(dataSource)
                .sql("insert into item_rows (first_name, last_name, date) " +
                        "values (:firstName, :lastName, :date)")
                .beanMapped()
                .build();
    }


    @Bean
    public Step deleteUnzippedFilesStep(StepBuilderFactory stepBuilderFactory, Tasklet deleteUnzippedFilesTasklet) {
        return stepBuilderFactory.get("delete-unzipped-files")
                .tasklet(deleteUnzippedFilesTasklet)
                .build();
    }

    @Bean
    public Tasklet deleteUnzippedFilesTasklet() {
        return (stepContribution, chunkContext) -> {
            FileSystemUtils.deleteRecursively(Path.of("src/main/resources/temp"));
            return RepeatStatus.FINISHED;
        };
    }


    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("spring_batch");
    }
}
