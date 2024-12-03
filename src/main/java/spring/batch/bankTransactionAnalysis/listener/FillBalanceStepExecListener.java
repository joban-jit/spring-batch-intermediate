package spring.batch.bankTransactionAnalysis.listener;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import spring.batch.bankTransactionAnalysis.processor.FillBalanceProcessor;

import javax.sql.DataSource;

import static spring.batch.bankTransactionAnalysis.constants.CommonConstants.NEGATIVE;
import static spring.batch.bankTransactionAnalysis.constants.CommonConstants.POSITIVE;

@Slf4j
@Component
public class FillBalanceStepExecListener {

    private final FillBalanceProcessor processor;
    private final DataSource postgresqlDataSource;
    private final PlatformTransactionManager postgresTransactionManager;

    public FillBalanceStepExecListener(
            @Qualifier("fillBalanceProcessor") FillBalanceProcessor processor,
            @Qualifier("postgresqlDataSource") DataSource postgresqlDataSource,
            @Qualifier("postgresTransactionManager") PlatformTransactionManager postgresTransactionManager
    ) {
        this.processor = processor;
        this.postgresqlDataSource = postgresqlDataSource;
        this.postgresTransactionManager = postgresTransactionManager;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        TransactionTemplate transactionTemplate = new TransactionTemplate(postgresTransactionManager);
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transactionTemplate.execute(status -> {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(this.postgresqlDataSource);
            String sql = "ALTER TABLE bank_transaction_yearly ADD COLUMN IF NOT EXISTS balance NUMERIC(10,2)";
            try {
                jdbcTemplate.update(sql);
            } catch (DataAccessException e) {
                log.error("Error adding balance column: {}", e.getMessage());
                throw e;
            }
            return null;
        });
        // needed for processing items
        processor.setStepExecution(stepExecution);
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        double totalBalance = processor.getLatestTransactionBalance();
        processor.setStepExecution(null);
        return new ExitStatus(totalBalance >= 0 ? POSITIVE : NEGATIVE);
    }


}
