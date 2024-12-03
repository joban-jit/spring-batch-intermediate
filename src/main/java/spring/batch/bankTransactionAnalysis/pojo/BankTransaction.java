package spring.batch.bankTransactionAnalysis.pojo;

import com.google.common.collect.ImmutableMap;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;


public record BankTransaction(
        long id,
        int month,
        int day,
        int hour,
        int minute,
        BigDecimal amount,
        String merchant
) {
    public static final String SELECT_ALL_QUERY = "select id, month, day, hour, minute, amount, merchant from bank_transaction_yearly";

    public static PostgresPagingQueryProvider getQueryProvider() {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("id, month, day, hour, minute, amount, merchant");
        queryProvider.setFromClause("bank_transaction_yearly");
        ImmutableMap<String, Order> sortKeysMap = ImmutableMap.<String, Order>builder()
                .put("id", Order.ASCENDING)
                .build();
        queryProvider.setSortKeys(sortKeysMap);
        return queryProvider;
    }

    public static PagingQueryProvider getCurrencyAdjustmentQueryProvider() {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("id, month, day, hour, minute, amount, merchant");
        queryProvider.setFromClause("bank_transaction_yearly");
        queryProvider.setWhereClause("adjusted=false");
        ImmutableMap<String, Order> sortKeysMap = ImmutableMap.<String, Order>builder()
                .put("id", Order.ASCENDING)
                .build();
        queryProvider.setSortKeys(sortKeysMap);
        return queryProvider;
    }


    public static final RowMapper<BankTransaction> ROW_MAPPER = (rs, rowNum) -> new BankTransaction(
            rs.getLong("id"),
            rs.getInt("month"),
            rs.getInt("day"),
            rs.getInt("hour"),
            rs.getInt("minute"),
            rs.getBigDecimal("amount"),
            rs.getString("merchant")

    );


}
