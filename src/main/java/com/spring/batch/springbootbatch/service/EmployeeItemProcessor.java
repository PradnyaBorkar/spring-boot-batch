package com.spring.batch.springbootbatch.service;

import com.spring.batch.springbootbatch.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class EmployeeItemProcessor implements ItemProcessor<Employee,Employee> {
    private final Logger logger = LoggerFactory.getLogger(EmployeeItemProcessor.class);

    @Override
    public Employee process(Employee item) throws Exception {
        logger.info("transforming employee : "+item.getFirstName() +" "+item.getLastName());
        return new Employee(item.getId(),item.getFirstName().toUpperCase(),item.getLastName().toUpperCase());
    }
}
