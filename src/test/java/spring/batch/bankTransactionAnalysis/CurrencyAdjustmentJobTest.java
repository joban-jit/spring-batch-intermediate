package spring.batch.bankTransactionAnalysis;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import spring.batch.bankTransactionAnalysis.config.CurrencyAdjustmentConfig;
import spring.batch.bankTransactionAnalysis.pojo.BankTransaction;
import spring.batch.bankTransactionAnalysis.utils.GenerateSourceDatabase;
import spring.batch.bankTransactionAnalysis.utils.SourceManagementUtils;

import javax.sql.DataSource;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@Slf4j
@SpringBatchTest
@SpringJUnitConfig({CurrencyAdjustmentConfig.class})
@TestPropertySource("classpath:application.properties")
public class CurrencyAdjustmentJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    @Qualifier("currencyAdjustmentJob")
    private Job currencyAdjustmentJob;

    @Value("${currency.adjustment.rate}")
    private double currencyAdjustmentRate;

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setDataSource(
            @Qualifier("postgresqlDataSource") DataSource dataSource
    ){
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }


    @BeforeEach
    public void initDatabase(){
        SourceManagementUtils.initializeEmptyDatabase(jdbcTemplate);
    }

    @Test
    public void testCurrencyAdjustmentJob() throws Exception {
        Random random = new Random();
        String[] merchants = new String[] {UUID.randomUUID().toString()};

        BankTransaction[] generatedTransactions = new BankTransaction[] {
                GenerateSourceDatabase.generateRecord(random, merchants),
                GenerateSourceDatabase.generateRecord(random, merchants),
                GenerateSourceDatabase.generateRecord(random, merchants)
        };

        for (BankTransaction generatedTransaction : generatedTransactions) {
            SourceManagementUtils.insertBankTransaction(generatedTransaction, jdbcTemplate);
        }

        // Configure job launcher to run the job
        jobLauncherTestUtils.setJob(currencyAdjustmentJob);
        // Launch the currency adjustment job
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        // Verify that job executed successfully
        Assertions.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        List<BankTransaction> dbTransactions = jdbcTemplate
                .query(BankTransaction.SELECT_ALL_QUERY, BankTransaction.ROW_MAPPER);

        for (BankTransaction dbTransaction : dbTransactions) {
            BankTransaction generatedTransaction = generatedTransactions[(int) dbTransaction.id() - 1];
            // We check whether each transaction amount was adjusted
            Assertions.assertEquals(dbTransaction.amount()
                            .divide(generatedTransaction.amount(), RoundingMode.HALF_UP)
                            .doubleValue(),
                    // Tiny delta due to precision of floating numbers in computers
                    currencyAdjustmentRate, 0.0000001);
        }
    }



}
