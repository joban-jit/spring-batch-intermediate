package spring.batch.bankTransactionAnalysis.processor;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemProcessor;
import spring.batch.bankTransactionAnalysis.pojo.BalanceUpdate;
import spring.batch.bankTransactionAnalysis.pojo.BankTransaction;

import java.math.BigDecimal;
import java.util.Objects;

import static spring.batch.bankTransactionAnalysis.constants.CommonConstants.BALANCE_SO_FAR;

public class FillBalanceProcessor implements ItemProcessor<BankTransaction, BalanceUpdate> {

    // Step execution variable needs to be set in order for processor to be executed
    private StepExecution stepExecution;

    @Override
    public BalanceUpdate process(BankTransaction item) throws Exception {

        if(Objects.isNull(stepExecution)){
            throw new RuntimeException("Can not process item without accessing the step execution");
        }
        BigDecimal newBalance = BigDecimal.valueOf(getLatestTransactionBalance() )
        .add(item.amount());
        BalanceUpdate balanceUpdate = new BalanceUpdate(item.id(),newBalance);
        stepExecution.getExecutionContext().putDouble(BALANCE_SO_FAR,newBalance.doubleValue());
        return balanceUpdate;
    }

    public double getLatestTransactionBalance() {
        if (stepExecution == null) {
            throw new RuntimeException("Can not get the latest balance without accessing the step execution");
        }
        // If no balance is present, start from 0
        return stepExecution.getExecutionContext().getDouble(BALANCE_SO_FAR, 0d);
    }

    // Step execution need to be set when step execution is relevant, and cleared when no longer relevant
    public void setStepExecution(StepExecution stepExecution){
        this.stepExecution = stepExecution;
    }
}
