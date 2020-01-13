package com.spring.batch.springbootbatch.configuration;

import com.spring.batch.springbootbatch.model.Employee;
import com.spring.batch.springbootbatch.service.EmployeeItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public FlatFileItemReader<Employee> reader(){
        FlatFileItemReader<Employee> csvFileReader = new FlatFileItemReaderBuilder<Employee>()
                .name("itemReader")
                .resource(new ClassPathResource("/in/sample-data.csv"))
                .delimited()
                .names(new String[]{"id","firstName", "lastName"})
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Employee>() {{
                    setTargetType(Employee.class);
                }}).build();
        csvFileReader.setLinesToSkip(1);
        return csvFileReader;
    }

    @Bean
    public EmployeeItemProcessor itemProcessor(){
        return new EmployeeItemProcessor();
    }

    @Bean
    public FlatFileItemWriter<Employee> itemWriter(){
        return new FlatFileItemWriterBuilder<Employee>().
                name("itemWriter")
                .resource(new ClassPathResource("/out/final-data.csv"))
                .delimited()
                .names(new String[]{"id","firstName", "lastName"})
                .lineAggregator(new DelimitedLineAggregator<Employee>() {
                {
                    setDelimiter(",");
                    setFieldExtractor(new BeanWrapperFieldExtractor<Employee>() {
                    { setNames(new String[] { "id", "firstName", "lastName" }); }
                });
            }}).build();
    }

    @Bean
    public Job readCSVFilesJob() {
        return jobBuilderFactory
                .get("readCSVFilesJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Employee, Employee> chunk(10)
                .reader(reader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }
}
