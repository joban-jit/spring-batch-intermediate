package spring.batch.bankTransactionAnalysis.utils;

import org.springframework.jdbc.core.JdbcTemplate;
import spring.batch.bankTransactionAnalysis.pojo.BankTransaction;

public class SourceManagementUtils {

    public static void initializeEmptyDatabase(JdbcTemplate jdbcTemplate) {
        // Drop table if exist
        jdbcTemplate.update("drop table if exists bank_transaction_yearly");
        // Create the table
        jdbcTemplate.update("create table bank_transaction_yearly (" +
                "id serial primary key," +
                "month int not null," +
                "day int not null," +
                "hour int not null," +
                "minute int not null," +
                "amount numeric(10,2) not null," +
                "merchant varchar(36) not null" +
                ")");
    }
    public static void insertBankTransaction(BankTransaction transaction, JdbcTemplate jdbcTemplate) {
        jdbcTemplate.update("insert into bank_transaction_yearly (month, day, hour, minute, amount, merchant) " +
                        "values (?, ?, ?, ?, ?, ?)",
                transaction.month(), transaction.day(), transaction.hour(),
                transaction.minute(), transaction.amount(), transaction.merchant());
    }
}
