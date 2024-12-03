package spring.batch.bankTransactionAnalysis;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;
import spring.batch.bankTransactionAnalysis.pojo.BankTransaction;
import spring.batch.bankTransactionAnalysis.processor.FillBalanceProcessor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static spring.batch.bankTransactionAnalysis.constants.CommonConstants.BALANCE_SO_FAR;
// here we don't need application context of spring batch as these are simple junit tests
public class FillBalanceProcessorTest {

    private static final Random RANDOM = new Random();

    @Test
    void testProcessorWithMetadataInstanceFactory() throws Exception {
        double balanceSoFar = RANDOM.nextDouble();
        BigDecimal transactionAmount = BigDecimal.valueOf(balanceSoFar);

        BankTransaction bankTransaction = new BankTransaction(1, 1, 1, 1, 1,
                transactionAmount, UUID.randomUUID().toString());

        FillBalanceProcessor processor = new FillBalanceProcessor();
        StepExecution stepExecution = MetaDataInstanceFactory.createStepExecution();
        ExecutionContext executionContext = initExecutionContext(balanceSoFar);
        stepExecution.setExecutionContext(executionContext);
        processor.setStepExecution(stepExecution);
        processor.process(bankTransaction);
        Assertions.assertEquals(stepExecution.getExecutionContext()
                        .getDouble(BALANCE_SO_FAR),
                transactionAmount.add(BigDecimal.valueOf(balanceSoFar))
                        .setScale(2, RoundingMode.HALF_UP)
                        .doubleValue(),
                // Implementation only cares about scale of 2
                0.01);

    }

    @Test
    void testProcessorWithMockito() throws Exception {
        double balanceSoFar = RANDOM.nextDouble();
        BigDecimal transactionAmount = BigDecimal.valueOf(balanceSoFar);

        BankTransaction bankTransaction = new BankTransaction(1, 1, 1, 1, 1,
                transactionAmount, UUID.randomUUID().toString());

        FillBalanceProcessor processor = new FillBalanceProcessor();
        StepExecution stepExecutionMock = Mockito.mock(StepExecution.class);
        ExecutionContext executionContext = initExecutionContext(balanceSoFar);
        stepExecutionMock.setExecutionContext(executionContext);
        Mockito.when(stepExecutionMock.getExecutionContext()).thenReturn(executionContext);
        processor.setStepExecution(stepExecutionMock);
        processor.process(bankTransaction);
        assertEquals(transactionAmount.add(BigDecimal.valueOf(balanceSoFar)).setScale(2, RoundingMode.HALF_UP).doubleValue(),
                stepExecutionMock.getExecutionContext().getDouble(BALANCE_SO_FAR),0.01
                );

    }

    private ExecutionContext initExecutionContext(double value){
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.putDouble(BALANCE_SO_FAR, value);
        return executionContext;
    }

}
