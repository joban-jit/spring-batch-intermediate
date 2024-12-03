package spring.batch.bankTransactionAnalysis.pojo;

import java.math.BigDecimal;

public record BalanceUpdate(
        long id,
        BigDecimal balance) {
}
