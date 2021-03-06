package pt.isel.ngspipes.engine_common.commandBuilders;


import pt.isel.ngspipes.engine_common.entities.Environment;
import pt.isel.ngspipes.engine_common.entities.contexts.ExecutionContext;
import pt.isel.ngspipes.engine_common.entities.contexts.Job;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;
import pt.isel.ngspipes.engine_common.entities.contexts.SimpleJob;
import pt.isel.ngspipes.engine_common.exception.CommandBuilderException;

import java.util.AbstractMap;
import java.util.Map;

public class DockerCommandBuilder extends CommandBuilder {

    // 1: engineStepOutputDir 2:engineStepInputDir 3:imageName 4:stepCommand
    private static final String DOCKER_CMD = "sudo docker run -w /sharedOutputs/ -v %1$s:/sharedOutputs/:%2$s -v " +
                                             "%3$s:/sharedInputs/:%2$s %4$s %5$s";
    private static final String DOCKER_IMG_NAME_KEY = "uri";
    private static final String DOCKER_IMG_TAG_KEY = "tag";

    private final String volumePermission;

    public DockerCommandBuilder() { this("rw"); }

    public DockerCommandBuilder(String volumePermission) { this.volumePermission = volumePermission; }

    @Override
    public String build(Pipeline pipeline, Job job, String fileSeparator,
                        Map<String, Object> contextConfig) throws CommandBuilderException {
        this.fileSeparator = fileSeparator;
        Environment environment = job.getEnvironment();
        String dockerImage = getDockerImageName(((SimpleJob)job).getExecutionContext());
        String executionCommand = buildCommand(pipeline, job, this::getDockerInputValue);
        return String.format(DOCKER_CMD, environment.getOutputsDirectory(), volumePermission,
                                            pipeline.getEnvironment().getWorkDirectory(),
                                        dockerImage, executionCommand);
    }

    private String getDockerInputValue(AbstractMap.SimpleEntry<Job, String> entry, String value) {
        if (value.contains(","))
            return getFileArrayInputValue(entry, value);
        return getSimpleInputValue(entry, value);
    }

    private String getSimpleInputValue(AbstractMap.SimpleEntry<Job, String> entry, String value) {
//        int begin = value.lastIndexOf(fileSeparator);
//        String inputName = begin != -1 ? value.substring(begin + 1) : value;
        String folder = entry.getKey().getEnvironment().getWorkDirectory().replace(entry.getValue(), "");
//        return fileSeparator + "sharedInputs" + folder + fileSeparator + inputName;
        return fileSeparator + "sharedInputs" + folder + fileSeparator + value;
    }

    private String getDockerImageName(ExecutionContext execContext) throws CommandBuilderException {
        Map<String, Object> config = execContext.getConfig();
        if (!config.containsKey(DOCKER_IMG_NAME_KEY))
            throw new CommandBuilderException("Docker execution context must contain a configuration (uri) specifying docker image.");
        String uri = config.get(DOCKER_IMG_NAME_KEY).toString();
        if (!config.containsKey(DOCKER_IMG_TAG_KEY))
            return uri;
        return uri + ":" + config.get(DOCKER_IMG_TAG_KEY).toString();
    }

}
