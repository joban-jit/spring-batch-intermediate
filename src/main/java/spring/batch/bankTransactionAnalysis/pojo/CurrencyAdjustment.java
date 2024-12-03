package spring.batch.bankTransactionAnalysis.pojo;

import java.math.BigDecimal;

public record CurrencyAdjustment(
        long id,
        BigDecimal adjustedAmount
) {
}
