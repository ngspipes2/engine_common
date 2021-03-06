package pt.isel.ngspipes.engine_common.commandBuilders;

import pt.isel.ngspipes.engine_common.entities.contexts.Job;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.exception.CommandBuilderException;

import java.util.Arrays;
import java.util.Map;

public class LocalCommandBuilder extends CommandBuilder {

    @Override
    public String build(Pipeline pipeline, Job job, String fileSeparator,
                        Map<String, Object> contextConfig) throws CommandBuilderException {
        this.fileSeparator = fileSeparator;
        String setup = Arrays.toString((Object[]) contextConfig.get("setup"));
        setup = setup.equalsIgnoreCase("null") ? "" : setup;
        return setup + buildCommand(pipeline, job, this::getChainFileValue);
    }

}
