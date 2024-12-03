package spring.batch.bankTransactionAnalysis.pojo;

import com.google.common.collect.ImmutableMap;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;

public record MerchantMonthBalance(
        int month,
        String merchant,
        BigDecimal balance
) {
    public static PagingQueryProvider getQueryProvider(){
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("sum(amount) as balance, merchant, month");
        queryProvider.setFromClause("bank_transaction_yearly");
        queryProvider.setGroupClause("month, merchant");
        ImmutableMap<String, Order> sortKeyMap = ImmutableMap.<String, Order>builder()
                .put("month", Order.ASCENDING)
                .put("merchant", Order.ASCENDING)
                .build();
        queryProvider.setSortKeys(sortKeyMap);
        return queryProvider;
    }

    public static final RowMapper<MerchantMonthBalance> ROW_MAPPER = (rs, rowNum) -> new MerchantMonthBalance(
            rs.getInt("month"),
            rs.getString("merchant"),
            rs.getBigDecimal("balance")
    );


}
