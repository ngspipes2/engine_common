package pt.isel.ngspipes.engine_common.interfaces;

import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.exception.ExecutorException;

public interface IExecutor {

    void execute(Pipeline pipeline) throws ExecutorException;
    boolean stop(String executionId) throws ExecutorException;
    boolean clean(String executionId) throws ExecutorException;
    boolean cleanAll() throws ExecutorException;
    void getPipelineOutputs(String executionId, String outputDirectory) throws ExecutorException;
    String getWorkingDirectory();

}
