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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import spring.batch.bankTransactionAnalysis.config.BankTransferConfig;
import spring.batch.bankTransactionAnalysis.listener.FillBalanceStepExecListener;
import spring.batch.bankTransactionAnalysis.pojo.BankTransaction;
import spring.batch.bankTransactionAnalysis.utils.GenerateSourceDatabase;
import spring.batch.bankTransactionAnalysis.utils.SourceManagementUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static spring.batch.bankTransactionAnalysis.constants.CommonConstants.NEGATIVE;
import static spring.batch.bankTransactionAnalysis.constants.CommonConstants.POSITIVE;

@Slf4j
@SpringBatchTest
@SpringJUnitConfig({BankTransferConfig.class, FillBalanceStepExecListener.class})
@TestPropertySource("classpath:application.properties")
public class FillBalanceStepTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    @Qualifier("bankTransactionAnalysisJob")
    private Job bankTransactionAnalysisJob;

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public void setDataSource(@Qualifier("postgresqlDataSource") DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @BeforeEach // Before each @Test, initialize the database
    public void initDatabase() {
        SourceManagementUtils.initializeEmptyDatabase(jdbcTemplate);
    }


    @Test
    public void testFillTheBalanceStep() {
        Random random = new Random();
        int transactionCount = random.nextInt(200) + 1; // Testing for at least one transaction
        List<BankTransaction> generatedTransactions = new ArrayList<>(transactionCount);

        // Generate test bank transactions
        for (int i = 0; i < transactionCount; i++) {
            generatedTransactions.add(GenerateSourceDatabase.generateRecord(random, new String[]{UUID.randomUUID().toString()}));
        }

        // Insert test bank transactions
        for (BankTransaction generatedTransaction : generatedTransactions) {
            SourceManagementUtils.insertBankTransaction(generatedTransaction, jdbcTemplate);
        }
        jobLauncherTestUtils.setJob(bankTransactionAnalysisJob);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("fill-balance");

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        List<BigDecimal> dbBalanceList = jdbcTemplate.query("select balance from bank_transaction_yearly order by id", (rs, rowNum) -> rs.getBigDecimal("balance"));

        assertEquals(generatedTransactions.size(), dbBalanceList.size());

        final BigDecimal[] runningBalance = {BigDecimal.ZERO};

        Iterator<BigDecimal> dbBalanceIterator = dbBalanceList.iterator();
        generatedTransactions.stream()
                .map(BankTransaction::amount)
                .iterator().forEachRemaining(txnAmt->{
                    runningBalance[0] = runningBalance[0].add(txnAmt);
                    assertEquals(runningBalance[0], dbBalanceIterator.next());
                });
        String expectedExitCode = runningBalance[0].compareTo(BigDecimal.ZERO) < 0 ? NEGATIVE : POSITIVE;
        assertEquals(jobExecution.getExitStatus().getExitCode(), expectedExitCode);


    }

    @Test
    public void testFillTheBalanceStepWithEmptyTable() {
        // Configure job launcher to run the job
        jobLauncherTestUtils.setJob(bankTransactionAnalysisJob);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("fill-balance");

        // Verify that the table still has 0 records
        List<BankTransaction> dbTransactions = jdbcTemplate
                .query(BankTransaction.SELECT_ALL_QUERY, BankTransaction.ROW_MAPPER);
        Assertions.assertEquals(dbTransactions.size(), 0);

        // Verify that job executed successfully
        Assertions.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // Verify that exit code is positive (corner case scenario)
        Assertions.assertEquals(jobExecution.getExitStatus().getExitCode(), POSITIVE);
    }

}
