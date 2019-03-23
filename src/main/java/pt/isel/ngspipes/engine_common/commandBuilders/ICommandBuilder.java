package pt.isel.ngspipes.engine_common.commandBuilders;

import pt.isel.ngspipes.engine_common.entities.contexts.Job;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.exception.CommandBuilderException;

import java.util.Map;

public interface ICommandBuilder {

    String build(Pipeline pipeline, Job job, String fileSeparator, Map<String, Object> contextConfig) throws CommandBuilderException;
}
