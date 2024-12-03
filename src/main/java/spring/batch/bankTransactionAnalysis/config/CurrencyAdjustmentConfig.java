package spring.batch.bankTransactionAnalysis.config;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import spring.batch.bankTransactionAnalysis.pojo.BankTransaction;
import spring.batch.bankTransactionAnalysis.pojo.CurrencyAdjustment;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
@Configuration
@Import({DataSourceConfig.class, CommonBatchJobConfig.class})
public class CurrencyAdjustmentConfig
        extends DefaultBatchConfiguration
{



    @Bean("currencyAdjustmentJob")
    public Job currencyAdjustmentJob(JobRepository jobRepository,
                                     @Qualifier("currencyAdjustmentStep") Step currencyAdjustmentStep
    ) {
        log.debug("Initializing currencyAdjustmentJob");
        return new JobBuilder("currencyAdjustmentJob", jobRepository)
                .start(currencyAdjustmentStep)
                .build();
    }

    @Bean("currencyAdjustmentStep")
    public Step currencyAdjustmentStep(
            JobRepository jobRepository,
            @Qualifier("postgresqlDataSource") DataSource dataSource,
            PlatformTransactionManager transactionManager,
            @Qualifier("postgresTransactionManager") PlatformTransactionManager postgresTransactionManager,
            @Qualifier("currencyAdjustmentJdbcReader") ItemReader<BankTransaction> itemReader,
            @Value("${currency.adjustment.rate}") double rate,
            @Value("${currency.adjustment.disallowed.merchant}") String disallowedMerchant
    ) {
        return new StepBuilder("currency-adjustment", jobRepository)
                .<BankTransaction, CurrencyAdjustment>chunk(1, postgresTransactionManager)
                .reader(itemReader)
                .processor(item ->
                        new CurrencyAdjustment(
                                item.id(),
                                item.amount().multiply(BigDecimal.valueOf(rate)).setScale(2, RoundingMode.HALF_UP)
                        )
                )
                .writer(
                        new JdbcBatchItemWriterBuilder<CurrencyAdjustment>()
                                .dataSource(dataSource)
                                .itemPreparedStatementSetter(
                                        (item, ps) -> {
                                            ps.setBigDecimal(1, item.adjustedAmount());
                                            ps.setBoolean(2, true);
                                            ps.setLong(3, item.id());
                                        }
                                )
                                .sql("update bank_transaction_yearly set amount=?, adjusted=? where id =?")
                                .build()
                )
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        TransactionTemplate transactionTemplate = new TransactionTemplate(postgresTransactionManager);
                        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                        transactionTemplate.execute(txnStatus -> {
                            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                            jdbcTemplate.update("ALTER TABLE bank_transaction_yearly ADD COLUMN IF NOT EXISTS adjusted BOOLEAN DEFAULT FALSE");
                            return null;
                        });

                    }
                })
                .listener(new ItemReadListener<BankTransaction>() {
                    @Override
                    public void afterRead(BankTransaction item) {
                        if (disallowedMerchant.equals(item.merchant())) {
                            throw new RuntimeException("Disallowed merchant!");
                        }
                    }
                })
                .allowStartIfComplete(true)
                .build();

    }

    @Bean("currencyAdjustmentJdbcReader")
    public JdbcPagingItemReader<BankTransaction> currencyAdjustmentJdbcReader(
            @Qualifier("postgresqlDataSource") DataSource dataSource
    ) {
        PostgresPagingQueryProvider queryProvider = BankTransaction.getQueryProvider();
        queryProvider.setWhereClause("adjusted = false");
        return new JdbcPagingItemReaderBuilder<BankTransaction>()
                .dataSource(dataSource)
                .name("bankTransactionReader")
                .queryProvider(queryProvider)
                .rowMapper(BankTransaction.ROW_MAPPER)
                .pageSize(2)
                .saveState(false)
                .build();

    }


}
