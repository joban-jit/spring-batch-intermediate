package spring.batch.bankTransactionAnalysis.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;
import spring.batch.bankTransactionAnalysis.listener.FillBalanceStepExecListener;
import spring.batch.bankTransactionAnalysis.pojo.BalanceUpdate;
import spring.batch.bankTransactionAnalysis.pojo.BankTransaction;
import spring.batch.bankTransactionAnalysis.pojo.DailyBalance;
import spring.batch.bankTransactionAnalysis.pojo.MerchantMonthBalance;
import spring.batch.bankTransactionAnalysis.processor.FillBalanceProcessor;

import javax.sql.DataSource;

import static spring.batch.bankTransactionAnalysis.constants.CommonConstants.NEGATIVE;
import static spring.batch.bankTransactionAnalysis.constants.CommonConstants.POSITIVE;

@Configuration
@Import({DataSourceConfig.class, CommonBatchJobConfig.class})
public class BankTransferConfig extends DefaultBatchConfiguration {

    @Value("file:merchant_month.json")
    private WritableResource merchantMonthlyBalanceJsonResource;

    @Value("file:daily_balance.json")
    private WritableResource dailyBalanceJsonResource;


    @Bean("bankTransactionAnalysisJob")
    public Job bankTransactionAnalysisJob(
            JobRepository jobRepository,
            @Qualifier("fillBalanceStep") Step fillBalanceStep,
            @Qualifier("aggregateByMerchantMonthlyStep") Step aggregateByMerchantMonthlyStep,
            @Qualifier("aggregateByDayStep") Step aggregateByDayStep
    ) {
        return new JobBuilder("bankTransactionAnalysisJob", jobRepository)
                .start(fillBalanceStep)
                .on(POSITIVE).to(aggregateByMerchantMonthlyStep)
                .from(fillBalanceStep)
                .on(NEGATIVE).to(aggregateByDayStep)
                .from(fillBalanceStep)
                .on("*")
                .end()
                .end()
                .build();
    }

    @Bean
    public Step fillBalanceStep(JobRepository jobRepository,
                                @Qualifier("postgresTransactionManager") PlatformTransactionManager transactionManager,
                                @Qualifier("postgresqlDataSource") DataSource dataSource,
                                @Qualifier("bankTransactionJdbcReader") ItemReader<BankTransaction> bankTransactionJdbcReader,
                                @Qualifier("fillBalanceProcessor") FillBalanceProcessor fillBalanceProcessor,
                                @Qualifier("balanceUpdateJdbcWriter") ItemWriter<BalanceUpdate> balanceUpdateJdbcWriter,
                                @Qualifier("fillBalanceStepExecListener") FillBalanceStepExecListener fillBalanceStepExecListener

    ) {
        return new StepBuilder("fill-balance", jobRepository)
                .<BankTransaction, BalanceUpdate>chunk(10, transactionManager)
                .reader(bankTransactionJdbcReader)
                .processor(fillBalanceProcessor)
                .writer(balanceUpdateJdbcWriter)
                .listener(fillBalanceStepExecListener)
                .allowStartIfComplete(true)
                .build();

    }

    @Bean("aggregateByMerchantMonthlyStep")
    public Step aggregateByMerchantMonthlyStep(
            JobRepository jobRepository,
            @Qualifier("postgresTransactionManager") PlatformTransactionManager transactionManager,
            @Qualifier("merchantMonthAggregationReader") ItemReader<MerchantMonthBalance> merchantMonthAggregationReader,
            @Qualifier("merchantMonthAggregationWriter") ItemWriter<MerchantMonthBalance> merchantMonthAggregationWriter
    ) {
        return new StepBuilder("aggregate-by-merchant-monthly", jobRepository)
                .<MerchantMonthBalance, MerchantMonthBalance>chunk(5, transactionManager)
                .reader(merchantMonthAggregationReader)
                .writer(merchantMonthAggregationWriter)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean("aggregateByDayStep")
    public Step aggregateByDayStep(
            JobRepository jobRepository,
            @Qualifier("postgresTransactionManager") PlatformTransactionManager transactionManager,
            @Qualifier("dailyBalanceAggregationReader") ItemReader<DailyBalance> dailyBalanceAggregationReader,
            @Qualifier("dailyBalanceAggregationWriter") ItemWriter<DailyBalance> dailyBalanceAggregationWriter

    ) {
        return new StepBuilder("aggregate-by-day", jobRepository)
                .<DailyBalance, DailyBalance>chunk(5, transactionManager)
                .reader(dailyBalanceAggregationReader)
                .writer(dailyBalanceAggregationWriter)
                .build();
    }

    // Readers
    @Bean("bankTransactionJdbcReader")
    public JdbcPagingItemReader<BankTransaction> bankTransactionJdbcReader(
            @Qualifier("postgresqlDataSource") DataSource dataSource
    ) {
        return new JdbcPagingItemReaderBuilder<BankTransaction>()
                .name("bankTransactionJdbcReader")
                .dataSource(dataSource)
                .queryProvider(BankTransaction.getQueryProvider())
                .rowMapper(BankTransaction.ROW_MAPPER)
                .pageSize(5)
                .build();

    }


    @Bean("merchantMonthAggregationReader")
    public ItemReader<MerchantMonthBalance> merchantMonthAggregationReader(
            @Qualifier("postgresqlDataSource") DataSource dataSource
    ) {
        return new JdbcPagingItemReaderBuilder<MerchantMonthBalance>()
                .name("merchantMonthAggregationReader")
                .dataSource(dataSource)
                .queryProvider(MerchantMonthBalance.getQueryProvider())
                .rowMapper(MerchantMonthBalance.ROW_MAPPER)
                .pageSize(5)
                .build();
    }

    @Bean("dailyBalanceAggregationReader")
    public ItemReader<DailyBalance> dailyBalanceAggregationReader(
            @Qualifier("postgresqlDataSource") DataSource dataSource
    ) {
        return new JdbcPagingItemReaderBuilder<DailyBalance>()
                .name("dailyBalanceAggregationReader")
                .dataSource(dataSource)
                .queryProvider(DailyBalance.getQueryProvider())
                .rowMapper(DailyBalance.ROW_MAPPER)
                .pageSize(5)
                .build();
    }


    // Writers
    @Bean("balanceUpdateJdbcWriter")
    public JdbcBatchItemWriter<BalanceUpdate> balanceUpdateJdbcWriter(
            @Qualifier("postgresqlDataSource") DataSource dataSource
    ) {
        return new JdbcBatchItemWriterBuilder<BalanceUpdate>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(item -> {
                    MapSqlParameterSource parameterSource = new MapSqlParameterSource();
                    parameterSource.addValue("balance", item.balance());
                    parameterSource.addValue("id", item.id());
                    return parameterSource;
                })
                .sql("update bank_transaction_yearly set balance = :balance where id = :id")
                .build();

    }

    @Bean("merchantMonthAggregationWriter")
    public ItemWriter<MerchantMonthBalance> merchantMonthAggregationWriter() {
        return new JsonFileItemWriterBuilder<MerchantMonthBalance>()
                .name("merchantMonthAggregationWriter")
                .resource(merchantMonthlyBalanceJsonResource)
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                .build();
    }


    @Bean("dailyBalanceAggregationWriter")
    public ItemWriter<DailyBalance> dailyBalanceAggregationWriter() {
        return new JsonFileItemWriterBuilder<DailyBalance>()
                .name("dailyBalanceAggregationWriter")
                .resource(dailyBalanceJsonResource)
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                .build();
    }

    @Bean
    public FillBalanceProcessor fillBalanceProcessor() {
        return new FillBalanceProcessor();
    }


}
