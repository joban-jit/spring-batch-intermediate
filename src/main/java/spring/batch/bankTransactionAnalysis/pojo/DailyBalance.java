package spring.batch.bankTransactionAnalysis.pojo;

import com.google.common.collect.ImmutableMap;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;

public record DailyBalance(
        int day,
        int month,
        BigDecimal balance
) {
    // Query provider to obtain daily balance aggregation from 'bank_transaction_yearly' table
    public static PagingQueryProvider getQueryProvider(){
        PostgresPagingQueryProvider postgresPagingQueryProvider = new PostgresPagingQueryProvider();
        postgresPagingQueryProvider.setSelectClause("sum(amount) as balance, day, month");
        postgresPagingQueryProvider.setFromClause("bank_transaction_yearly");
        postgresPagingQueryProvider.setGroupClause("day, month");
        postgresPagingQueryProvider.setSortKeys(ImmutableMap.<String, Order>builder()
                .put("month", Order.ASCENDING)
                .put("day", Order.ASCENDING)
                .build());
        return postgresPagingQueryProvider;
    }

    // Row mapper to transform query results into java object
    public static final RowMapper<DailyBalance> ROW_MAPPER = (rs, rowNum)-> new DailyBalance(
            rs.getInt("day"),
            rs.getInt("month"),
            rs.getBigDecimal("balance")
    );
}
