package spring.batch.bankTransactionAnalysis;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.UUID;

@SpringBootApplication
public class BankTransactionAnalysisApplication
        implements CommandLineRunner {


    @Autowired
    private JobLauncher jobLauncher;

//	@Autowired
//	private Job bankTransactionAnalysisJob;

    @Autowired
    private Job currencyAdjustmentJob;


    public static void main(String[] args) {
        SpringApplication.run(BankTransactionAnalysisApplication.class, args);
    }


    @Override
    public void run(String... args) throws Exception {
        String uniqueId = UUID.randomUUID().toString();
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("id", uniqueId)
                .toJobParameters();

        jobLauncher.run(currencyAdjustmentJob, jobParameters);
//		jobLauncher.run(bankTransactionAnalysisJob, jobParameters);

    }
}
