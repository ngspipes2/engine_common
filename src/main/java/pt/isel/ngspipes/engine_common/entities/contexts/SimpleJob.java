package pt.isel.ngspipes.engine_common.entities.contexts;

import com.fasterxml.jackson.annotation.JsonIgnore;
import pt.isel.ngspipes.engine_common.commandBuilders.ICommandBuilder;
import pt.isel.ngspipes.engine_common.entities.Environment;
import pt.isel.ngspipes.engine_common.utils.CommandBuilderSupplier;

public class SimpleJob extends Job {

    ExecutionContext executionContext;
    String command;

    public SimpleJob(String id, Environment environment, String command,
                     ExecutionContext executionContext) {
        super(id, environment);
        this.executionContext = executionContext;
        this.command = command;
    }

    public SimpleJob() {}

    @JsonIgnore
    public ICommandBuilder getCommandBuilder() { return supplyCommandBuilder(); }
    private ICommandBuilder supplyCommandBuilder() {
        return CommandBuilderSupplier.getCommandBuilder(executionContext.context);
    }

    public ExecutionContext getExecutionContext() { return executionContext; }

    public String getCommand() { return command; }
}
