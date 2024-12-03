package spring.batch.bankTransactionAnalysis.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Slf4j
@Configuration
@EnableConfigurationProperties
public class DataSourceConfig {


    @Bean(name = "commonHikariConfig")
    @ConfigurationProperties(prefix = "datasource.hikari.common")
    public HikariConfig commandHikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "postgresDataSourceProperties")
    @ConfigurationProperties(prefix = "postgres.db")
    public CommonDataSourceProperties postgresDataSourceProperties() {
        return new CommonDataSourceProperties();
    }

    @Bean(name = "mysqlDataSourceProperties")
    @ConfigurationProperties(prefix = "mysql.db")
    public CommonDataSourceProperties mysqlDataSourceProperties() {
        return new CommonDataSourceProperties();
    }


    @Bean(name = "postgresqlDataSource")
    public DataSource postgresDataSource(
            HikariConfig commonHikariConfig,
            @Qualifier("postgresDataSourceProperties") CommonDataSourceProperties dataSourceProperties
    ) {
        return buildDataSource(commonHikariConfig, dataSourceProperties);
    }


    @Primary
    @Bean(name = "dataSource")
    // Job repository data source should be named 'dataSource' as per DefaultBatchConfiguration
    public DataSource mysqlDataSource(
            HikariConfig commonHikariConfig,
            @Qualifier("mysqlDataSourceProperties") CommonDataSourceProperties dataSourceProperties
    ) {
        return buildDataSource(commonHikariConfig, dataSourceProperties);
    }

    @Bean(name = "postgresTransactionManager")
    public PlatformTransactionManager postgresTransactionManager(
            @Qualifier("postgresqlDataSource") DataSource dataSource
    ) {
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager();
        jdbcTransactionManager.setDataSource(dataSource);
        return jdbcTransactionManager;
    }


    @Primary
    @Bean(name = "transactionManager")
    public PlatformTransactionManager mysqlTransactionManager(
            @Qualifier("dataSource") DataSource dataSource
    ) {
        JdbcTransactionManager jdbcTransactionManager = new JdbcTransactionManager();
        jdbcTransactionManager.setDataSource(dataSource);
        return jdbcTransactionManager;
    }

    private static HikariDataSource buildDataSource(HikariConfig commonHikariConfig,
                                                    CommonDataSourceProperties dataSourceProperties) {
        HikariConfig hikariConfig = new HikariConfig();
        commonHikariConfig.copyStateTo(hikariConfig);
        log.info(String.valueOf(dataSourceProperties.toString()));
        hikariConfig.setJdbcUrl(dataSourceProperties.getUrl());
        hikariConfig.setUsername(dataSourceProperties.getUsername());
        hikariConfig.setPassword(dataSourceProperties.getPassword());
        hikariConfig.setDriverClassName(dataSourceProperties.getDriverClassName());
        hikariConfig.setPoolName(dataSourceProperties.getPoolName());
        return new HikariDataSource(hikariConfig);
    }

}
