package spring.batch.bankTransactionAnalysis.config;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;

@Getter
@Setter
@ToString
public class CommonDataSourceProperties extends DataSourceProperties {
    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private String poolName;
}
