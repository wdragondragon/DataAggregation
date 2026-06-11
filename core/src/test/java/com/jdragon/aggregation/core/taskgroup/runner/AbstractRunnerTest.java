package com.jdragon.aggregation.core.taskgroup.runner;

import com.jdragon.aggregation.core.enums.State;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class AbstractRunnerTest {

    @Test
    public void markSuccessShouldNotOverrideExistingFailure() {
        RuntimeException failure = new RuntimeException("reader failed");
        Communication communication = new Communication();
        communication.setState(State.FAILED);
        communication.setThrowable(failure);

        AbstractRunner runner = new AbstractRunner(null);
        runner.setRunnerCommunication(communication);

        runner.markSuccess();

        assertEquals(State.FAILED, communication.getState());
        assertSame(failure, communication.getThrowable());
    }
}
