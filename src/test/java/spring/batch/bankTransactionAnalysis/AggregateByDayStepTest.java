package spring.batch.bankTransactionAnalysis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import spring.batch.bankTransactionAnalysis.config.BankTransferConfig;
import spring.batch.bankTransactionAnalysis.listener.FillBalanceStepExecListener;
import spring.batch.bankTransactionAnalysis.pojo.BankTransaction;
import spring.batch.bankTransactionAnalysis.utils.SourceManagementUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBatchTest
@SpringJUnitConfig({
        BankTransferConfig.class,
        FillBalanceStepExecListener.class

})
@TestPropertySource("classpath:application.properties")
public class AggregateByDayStepTest {

    @Value("classpath:test/expected_day_aggregation.json")
    private Resource expectedAggregationJsonResource;

    @Value("file:daily_balance.json")
    private Resource dailyBalanceJsonResource;

    @Autowired
    @Qualifier("bankTransactionAnalysisJob")
    private Job bankTransactionAnalysisJob;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

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
    public void testAggregateByDayStep() throws Exception {
        List<BankTransaction> generatedTransactions = List.of (
                new BankTransaction(-1, 1, 13, 0, 0, new BigDecimal("2.5"), UUID.randomUUID().toString()),
                new BankTransaction(-1, 1, 13, 0, 0, new BigDecimal("2"), UUID.randomUUID().toString()),
                new BankTransaction(-1, 2, 17, 0, 0, new BigDecimal("8"), UUID.randomUUID().toString()),
                new BankTransaction(-1, 2, 17, 0, 0, new BigDecimal("6.11"), UUID.randomUUID().toString()),
                new BankTransaction(-1, 2, 29, 0, 0, new BigDecimal("-29.29"), UUID.randomUUID().toString())
        );

        generatedTransactions.forEach( txn->SourceManagementUtils.insertBankTransaction(txn, jdbcTemplate));

        jobLauncherTestUtils.setJob(bankTransactionAnalysisJob);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("aggregate-by-day");
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertTrue(jsonFilesEqualContent(expectedAggregationJsonResource, dailyBalanceJsonResource));

    }

    private boolean jsonFilesEqualContent(Resource actual, Resource expected) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualTree = mapper.readTree(actual.getFile());
        JsonNode expectedTree = mapper.readTree(expected.getFile());
        String prettyString = actualTree.toPrettyString();
        String expectedTreeString = expectedTree.toPrettyString();
        System.out.println(prettyString);
        System.out.println(expectedTreeString);
        return actualTree.equals(expectedTree);
    }

}
