package com.example.owox;

import com.example.owox.services.impl.DataSetOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class OwoxApplication {

	@Autowired
	public DataSetOptions dataSetOptions;

	public static void main(String[] args) throws Exception {
		SpringApplication.run(OwoxApplication.class, args);
	}

	@PostConstruct
	public void postConstruct() throws Exception {
		dataSetOptions.taskOne("coursera-162521", "proton5000", "tmp", 3);

		dataSetOptions.taskTwo("coursera-162521", "proton5000");
	}
}
