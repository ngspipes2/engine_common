package pt.isel.ngspipes.engine_common.entities.contexts;

import pt.isel.ngspipes.engine_common.entities.Environment;

public class SimpleJob extends Job {

    ExecutionContext executionContext;
    String command;

    public SimpleJob(String id, Environment environment, String command, ExecutionContext executionContext) {
        super(id, environment);
        this.executionContext = executionContext;
        this.command = command;
    }

    public SimpleJob() {}

    public ExecutionContext getExecutionContext() { return executionContext; }
    public String getCommand() { return command; }
}
