package pt.isel.ngspipes.engine_common.utils;

import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.*;
import pt.isel.ngspipes.engine_common.entities.contexts.strategy.ICombineStrategy;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.exception.InputValidationException;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IPipelineRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IToolRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.ICommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IPipelineExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.ICombineStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IInputStrategyDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.tool_descriptor.interfaces.ICommandDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IOutputDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IParameterDescriptor;
import pt.isel.ngspipes.tool_descriptor.interfaces.IToolDescriptor;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;

import java.io.File;
import java.util.*;

public class ValidateUtils {

    public static void validateJobs(List<Job> jobs) throws EngineCommonException {

        for (Job job : jobs) {
            if (job.getSpread() != null)
                validateSpread(job, job.getSpread());
        }
    }

    public static void validateOutputs(IPipelineDescriptor pipelineDesc, Map<String, Object> parameters) throws EngineCommonException {
        validateNoDuplicatedOutputsIds(pipelineDesc);
        validateOutputsExistence(pipelineDesc, parameters);
    }

    public static void validateRepositories(Collection<IRepositoryDescriptor> repositories) throws EngineCommonException {
        Collection<String> repoId = new LinkedList<>();
        for (IRepositoryDescriptor repositoryDescriptor : repositories) {
            if (repoId.contains(repositoryDescriptor.getId()))
                throw new EngineCommonException("Repositories ids can be duplicated. Id: " + repositoryDescriptor.getId() + " is duplicated.");
            else
                repoId.add(repositoryDescriptor.getId());
        }
    }

    public static void validateNonCyclePipeline(IPipelineDescriptor pipelineDescriptor, Map<String, Object> params) throws EngineCommonException {
        Collection<Map.Entry<String, String>> previousPipelines = new LinkedList<>();
        validateNonCycle(pipelineDescriptor,null, previousPipelines, params);
    }

    public static void validatePipelineState(Pipeline pipeline) throws EngineCommonException {
        ExecutionState executionState = pipeline.getState();
        if (executionState.getState().equals(StateEnum.FAILED))
            throw new EngineCommonException("Error validating pipeline.", executionState.getException());
    }

    public static void validateResources(Job job, Pipeline pipeline) throws EngineCommonException {
        int processors = Runtime.getRuntime().availableProcessors();
        long memory = Runtime.getRuntime().freeMemory();
        long disk = new File(job.getEnvironment().getOutputsDirectory()).getFreeSpace();
        String executionId = pipeline.getName();

        if (processors < job.getEnvironment().getCpu() ||
                processors < pipeline.getEnvironment().getCpu())
            throw new EngineCommonException("Needed cpus aren't available to execute step: " + job.getId()
                    + " from pipeline: " + executionId);

        if (memory < job.getEnvironment().getMemory() * 1024 ||
                memory < pipeline.getEnvironment().getMemory() * 1024)
            throw new EngineCommonException("Needed memory isn't available to execute step: " + job.getId()
                    + " from pipeline: " + executionId);

        if (disk < job.getEnvironment().getDisk() * 1024 * 1024 ||
                disk < pipeline.getEnvironment().getDisk() * 1024 * 1024)
            throw new EngineCommonException("Needed disk space isn't available to execute step: " + job.getId()
                    + " from pipeline: " + executionId);
    }

    public static void validateInput(IStepDescriptor step, ICommandDescriptor commandDescriptor,
                                     IParameterDescriptor parameterDescriptor, IInputDescriptor input,
                                     Map<String, IPipelinesRepository> pipelinesRepos,
                                     IPipelineDescriptor pipelineDescriptor, Map<String, Object> parameters) throws EngineCommonException {
        validateMandatory(step, commandDescriptor, parameterDescriptor, input, parameters);
        if (input != null)
            validateInputType(step, parameterDescriptor, input, pipelineDescriptor, pipelinesRepos, parameters);
    }

    static void validateInputValues(Map<String, List<String>> inputsValues, String first, String second) throws EngineCommonException {
        int sizeFirst = inputsValues.get(first).size();
        int sizeSecond = inputsValues.get(second).size();
        if (sizeFirst != sizeSecond)
            throw new EngineCommonException("Inputs to spread values must have the same quantity.");
    }


    private static void validateSpread(Job job, Spread spread) throws EngineCommonException {
        validateSpreadInputsExistence(job, spread.getInputs());
        validateSpreadStrategyInputsExistence(job, spread);
    }

    private static void validateSpreadStrategyInputsExistence(Job job, Spread spread) throws EngineCommonException {
        if (spread.getStrategy() != null) {
            Collection<String> inputs = spread.getInputs();
            if (inputs.size() < 2)
                throw new EngineCommonException("Inputs to spread must be at least 2.");
            int count = validateStrategyInput(job, inputs, spread.getStrategy());
            if (inputs.size() < count)
                throw new EngineCommonException("Inputs to spread must be " + count + ".");
        }
    }

    private static int validateStrategyInput(Job job, Collection<String> inputsToSpread,
                                             ICombineStrategy strategy) throws EngineCommonException {
        if (strategy == null)
            throw new EngineCommonException("Spread malformed on step" + job + ".");

        if (strategy instanceof ICombineStrategyDescriptor) {
            ICombineStrategyDescriptor combined = (ICombineStrategyDescriptor) strategy;
            int count = validateStrategyInput(job, inputsToSpread, (ICombineStrategy) combined.getFirstStrategy());
            return count + validateStrategyInput(job, inputsToSpread, (ICombineStrategy) combined.getSecondStrategy());
        } else if (strategy instanceof IInputStrategyDescriptor) {
            validateInputWithinToSpread(job.getId(), inputsToSpread, (IInputStrategyDescriptor) strategy);
            return 1;
        }

        return 0;
    }

    private static void validateInputWithinToSpread(String jobId, Collection<String> inputsToSpread,
                                                    IInputStrategyDescriptor strategy) throws EngineCommonException {
        String inputName = strategy.getInputName();
        if (!inputsToSpread.contains(inputName))
            throw new EngineCommonException("Error validating spread on step:" + jobId + ".Strategy input " + inputName
                    + " must be specified on spread inputs");
    }

    private static void validateSpreadInputsExistence(Job job, Collection<String> inputsToSpread) throws EngineCommonException {
        for (String inputName : inputsToSpread) {
            Input input = job.getInputById(inputName);
            if (input == null)
                throw new EngineCommonException("Using " + inputName + " as input to spread and " +
                        "isn't defined as input of step " + job.getId() + ".");
        }
    }

    private static void validateMandatory(IStepDescriptor step, ICommandDescriptor commandDescriptor,
                                          IParameterDescriptor parameterDescriptor, IInputDescriptor input,
                                          Map<String, Object> parameters) throws EngineCommonException {
        String depends = parameterDescriptor.getDepends();
        if (depends != null && !depends.isEmpty()) {
            validateDependent(step, commandDescriptor.getParameters(), parameterDescriptor, input, parameters);
        } else {
            validateRequired(step, parameterDescriptor, input);
        }
    }

    private static void validateInputType(IStepDescriptor step, IParameterDescriptor parameterDescriptor,
                                          IInputDescriptor input, IPipelineDescriptor pipeDesc,
                                          Map<String, IPipelinesRepository> pipelinesRepos,
                                          Map<String, Object> parameters) throws EngineCommonException {
        String paramType = parameterDescriptor.getType();

        if (paramType.equals("composed")) {
            validateComposedInput(step.getInputs(), parameterDescriptor);
            return;
        }

        if (step.getSpread() != null && step.getSpread().getInputsToSpread().contains(input.getInputName()))
            paramType += "[]";

        if (input instanceof IChainInputDescriptor) {
            IChainInputDescriptor chainInputDescriptor = ((IChainInputDescriptor) input);
            String stepId = chainInputDescriptor.getStepId();
            String baseId = stepId.contains("_") ? stepId.substring(0, stepId.indexOf("_")) : stepId;
            ChainInputDescriptor chain = instantiateChainInputDescriptor(pipeDesc, chainInputDescriptor, stepId);
            IPipelineDescriptor currPipelineDesc = pipeDesc;
            IStepDescriptor stepDesc = getStepDescriptor(pipeDesc, pipelinesRepos, baseId, chain.getStepId(), currPipelineDesc);
            IExecDescriptor exec = stepDesc.getExec();
            String repoId = exec.getRepositoryId();
            if (exec instanceof ICommandExecDescriptor) {
                IToolRepositoryDescriptor toolRepoDesc = DescriptorsUtils.getToolRepositoryDescriptorById(repoId, pipeDesc.getRepositories());
                IToolsRepository toolsRepo = RepositoryUtils.getToolsRepository(toolRepoDesc, parameters);
                ICommandExecDescriptor exec1 = (ICommandExecDescriptor) exec;
                IToolDescriptor toolDesc = DescriptorsUtils.getTool(toolsRepo, exec1.getToolName(), stepDesc.getId());
                ICommandDescriptor cmdDesc = DescriptorsUtils.getCommandByName(toolDesc.getCommands(), exec1.getCommandName());
                validateChainInputType(paramType, input, currPipelineDesc, cmdDesc, parameters, stepDesc);
            }
        } else {
            Object inputValue = DescriptorsUtils.getInputValue(input, parameters);
            validateInput(parameterDescriptor.getName(), paramType, inputValue);
        }
    }

    private static ChainInputDescriptor instantiateChainInputDescriptor(IPipelineDescriptor pipeDesc, IChainInputDescriptor chainInputDescriptor, String stepId) {
        ChainInputDescriptor chain = new ChainInputDescriptor();
        chain.setOutputName(chainInputDescriptor.getOutputName());
        chain.setInputName(chainInputDescriptor.getInputName());
        String id = getStepId(stepId, pipeDesc);
        chain.setStepId(id);
        return chain;
    }

    private static IStepDescriptor getStepDescriptor(IPipelineDescriptor pipeDesc, Map<String, IPipelinesRepository> pipelinesRepos,
                                                     String baseId, String id, IPipelineDescriptor currPipelineDesc) throws EngineCommonException {
        IStepDescriptor stepDesc = DescriptorsUtils.getStepById(pipeDesc, baseId);
        if (stepDesc.getExec() instanceof IPipelineExecDescriptor) {
            currPipelineDesc = DescriptorsUtils.getPipelineDescriptor(pipelinesRepos, stepDesc);
            stepDesc = DescriptorsUtils.getStepById(currPipelineDesc, id);
        }
        return stepDesc;
    }

    private static String getStepId(String stepId, IPipelineDescriptor pipeDesc) {
        if (!stepId.contains("_"))
            return stepId;
        int lastIdx = stepId.lastIndexOf("_");
        String base = stepId.substring(0, lastIdx);
        if (DescriptorsUtils.getStepById(pipeDesc, base).getExec() instanceof IPipelineExecDescriptor)
            return stepId.substring(lastIdx + 1);
        return base;
    }

    private static void validateComposedInput(Collection<IInputDescriptor> inputs, IParameterDescriptor parameterDescriptor) throws EngineCommonException {
        for (IParameterDescriptor subParameter : parameterDescriptor.getSubParameters()) {
            IInputDescriptor inputByName = DescriptorsUtils.getInputByName(inputs, subParameter.getName());
            if (subParameter.isRequired() && inputByName == null)
                throw new EngineCommonException("Input " + subParameter.getName() + " wasn't specified and is require.");
        }
    }

    private static void validateChainInputType(String paramType, IInputDescriptor input, IPipelineDescriptor pipelineDescriptor,
                                               ICommandDescriptor commandDescriptor, Map<String, Object> parameters,
                                               IStepDescriptor step) throws EngineCommonException {
        IChainInputDescriptor chainInput = (IChainInputDescriptor) input;
        String oputputType;

        if (step.getExec() instanceof ICommandExecDescriptor) {
            oputputType = getOutputTypeFromTool(chainInput, commandDescriptor);
        } else {
            oputputType = getOutputTypeFromPipeline(chainInput, commandDescriptor, pipelineDescriptor, parameters);
        }
        if (!paramType.equals(oputputType) && (paramType.equalsIgnoreCase("file_prefix") && !oputputType.contains("file")))
            throw new EngineCommonException("Chained input: " + input.getInputName() + " type doesn't verify with output: "
                    + oputputType + " type.");
    }

    private static String getOutputTypeFromPipeline(IChainInputDescriptor chainInput, ICommandDescriptor commandDescriptor,
                                                    IPipelineDescriptor pipelineDescriptor, Map<String, Object> params) throws EngineCommonException {

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output : pipelineDescriptor.getOutputs()) {
            if (output.getOutputName().equals(chainInput.getOutputName())) {
                IStepDescriptor step = DescriptorsUtils.getStepById(pipelineDescriptor, output.getStepId());
                if (step instanceof SimpleJob) {
                    return getOutputTypeFromTool(chainInput, commandDescriptor);
                } else {
                    assert step != null;
                    IPipelineDescriptor pipelineDesc = DescriptorsUtils.getPipelineDescriptor(pipelineDescriptor, params, step);
                    ICommandDescriptor cmdDescriptor = DescriptorsUtils.getCommandDescriptor(step, pipelineDesc, params);
                    return getOutputTypeFromPipeline(chainInput, cmdDescriptor, pipelineDesc, params);
                }
            }
        }

        throw new EngineCommonException("Error validating chain input " + chainInput.getInputName());
    }

    private static String getOutputTypeFromTool(IChainInputDescriptor chainInput, ICommandDescriptor cmdDesc) throws EngineCommonException {
        IOutputDescriptor output = DescriptorsUtils.getOutputFromCommand(cmdDesc, chainInput.getOutputName());
        return output.getType();
    }

    private static void validateNonCycle(IPipelineDescriptor pipelineDesc, IPipelineRepositoryDescriptor pipelineRepoDesc,
                                         Collection<Map.Entry<String, String>> previousPipelines, Map<String, Object> params) throws EngineCommonException {

        if (pipelineRepoDesc != null) {
            String pipelineName = pipelineDesc.getName();
            String location = pipelineRepoDesc.getLocation();

            AbstractMap.SimpleEntry<String, String> currentPipeline = new AbstractMap.SimpleEntry<>(pipelineName, location);
            if (previousPipelines.contains(currentPipeline))
                throw new EngineCommonException("It is not permit cycles on pipeline." + getCycleMessage(previousPipelines, currentPipeline));
            else
                previousPipelines.add(currentPipeline);
        }

        for (IStepDescriptor step : pipelineDesc.getSteps()) {
            if (step.getExec() instanceof IPipelineExecDescriptor) {
                IPipelineExecDescriptor exec = (IPipelineExecDescriptor) step.getExec();
                Collection<IRepositoryDescriptor> repositories = pipelineDesc.getRepositories();
                IPipelineRepositoryDescriptor repoDesc = DescriptorsUtils.getPipelineRepositoryDescribtorById(exec.getRepositoryId(), repositories);
                assert repoDesc != null;
                IPipelinesRepository pipeRepo = RepositoryUtils.getPipelinesRepository(repoDesc, params);
                IPipelineDescriptor subPipeDesc = DescriptorsUtils.getPipelineDescriptor(pipeRepo, exec.getPipelineName());
                validateNonCycle(subPipeDesc, repoDesc, new LinkedList<>(previousPipelines), params);
            }
        }
    }

    private static String getCycleMessage(Collection<Map.Entry<String, String>> previousPipelines,
                                          AbstractMap.SimpleEntry<String, String> currentPipeline) {
        StringBuilder cycle = new StringBuilder();
        StringBuilder tab = new StringBuilder();

        for (Map.Entry entry : previousPipelines) {
            cycle.append(tab).append(entry.getValue()).append(" - ").append(entry.getKey()).append("\n");
            tab.append("\t");
        }

        cycle.append(tab).append(currentPipeline.getValue()).append(" - ").append(currentPipeline.getKey());
        return cycle.toString();
    }

    private static void validateOutputsExistence(IPipelineDescriptor pipelineDescriptor,
                                                 Map<String, Object> parameters) throws EngineCommonException {

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDescriptor : pipelineDescriptor.getOutputs()) {

            IStepDescriptor step = DescriptorsUtils.getStepById(pipelineDescriptor, outputDescriptor.getStepId());
            String outputName = outputDescriptor.getOutputName();

            assert step != null;
            if (step.getExec() instanceof ICommandExecDescriptor) {
                validateExistenceOutputOnCommandStep(step, outputName, pipelineDescriptor, parameters);
            } else if (step instanceof IPipelineExecDescriptor) {
                validateExistenceOutputOnPipelineStep(step, outputName, pipelineDescriptor, parameters);
            } else {
                throw new EngineCommonException("No existent implementation was found for verifying output " + outputDescriptor.getName());
            }
        }
    }

    private static void validateExistenceOutputOnPipelineStep(IStepDescriptor step, String outputName, IPipelineDescriptor pipelineDesc,
                                                              Map<String, Object> parameters) throws EngineCommonException {

        IPipelineDescriptor pipeDesc = DescriptorsUtils.getPipelineDescriptor(pipelineDesc, parameters, step);
        if (pipeDesc.getOutputs().stream().anyMatch( (out) -> out.getName().equals(outputName)))
            return;
        throw new EngineCommonException("Output " + outputName + " doesn't exist on pipeline." + step.getId());
    }

    private static void validateExistenceOutputOnCommandStep(IStepDescriptor step, String outputName, IPipelineDescriptor pipelineDesc,
                                                             Map<String, Object> parameters) throws EngineCommonException {

        ICommandDescriptor commandDescriptor = DescriptorsUtils.getCommandDescriptor(step, pipelineDesc, parameters);
        IOutputDescriptor output = DescriptorsUtils.getOutputFromCommand(commandDescriptor, outputName);

        if (output.getValue().contains("$"))
            validateDependentOutput(output, step);
    }

    private static void validateDependentOutput(IOutputDescriptor output, IStepDescriptor step) throws EngineCommonException {

        String outputValue = output.getValue();

        if (outputValue.indexOf("$") != outputValue.lastIndexOf("$")) {
            String[] splittedByDependency = outputValue.split("$");
            for (String val : splittedByDependency) {
                validateExistentOfDependentInput(step, val);
            }
        } else {
            String val = outputValue.substring(outputValue.indexOf("$") + 1);
            validateExistentOfDependentInput(step, val);
        }
    }

    private static void validateExistentOfDependentInput(IStepDescriptor step, String str) throws EngineCommonException {
        boolean contains = isOutputDependentInputSpecified(str, step.getInputs());
        if (!contains)
            throw new EngineCommonException("Error validating output, dependent input wasn't specified.");

    }

    private static void validateNoDuplicatedOutputsIds(IPipelineDescriptor pipelineDescriptor) throws EngineCommonException {
        Collection<String> outputsIds = new LinkedList<>();

        for (pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor outputDescriptor : pipelineDescriptor.getOutputs()) {
            if (outputsIds.contains(outputDescriptor.getOutputName()))
                throw new EngineCommonException("Outputs ids can be duplicated. Id: " + outputDescriptor.getOutputName() + " is duplicated.");
            outputsIds.add(outputDescriptor.getName());
        }
    }

    private static void validateDependent(IStepDescriptor step, Collection<IParameterDescriptor> parameterDescriptors,
                                          IParameterDescriptor parameterDescriptor, IInputDescriptor input,
                                          Map<String, Object> parameters) throws EngineCommonException {
        String depends = parameterDescriptor.getDepends().replace("$", "");
        IInputDescriptor inputByName = DescriptorsUtils.getInputByName(step.getInputs(), depends);
        IParameterDescriptor rootParameterDescriptor = DescriptorsUtils.getParameterById(parameterDescriptors, depends);
        if (input != null && (inputByName == null || rootParameterDescriptor == null))
            throw new EngineCommonException("Input: " + parameterDescriptor.getName() + " depends on " + depends +
                    ".No input with name: " + depends + " was found.");
        if (parameterDescriptor.getDependentValues() != null && !parameterDescriptor.getDependentValues().isEmpty()) {
            Object rootValue = DescriptorsUtils.getInputValue(inputByName, parameters);
            boolean hasDependentValue = parameterDescriptor.getDependentValues().contains(rootValue.toString());
            if (input != null && !hasDependentValue)
                throw new EngineCommonException("Input: " + parameterDescriptor.getName() + " depends on " + depends +
                        "but value: " + rootValue + " is not one of its dependent values.");
            else if (hasDependentValue && parameterDescriptor.isRequired() && input == null)
                throw new EngineCommonException("Input: " + parameterDescriptor.getName() + " on step: " + step.getId() + " is required.");

        }
    }

    private static void validateRequired(IStepDescriptor step, IParameterDescriptor parameterDescriptor,
                                         IInputDescriptor input) throws EngineCommonException {
        if (parameterDescriptor.isRequired() && input == null)
            throw new EngineCommonException("Input: " + parameterDescriptor.getName() +
                    " is required and is not defined on step: " + step.getId() + ".");
    }

    private static void validateInput(String paramName, String paramType, Object inputValue) throws EngineCommonException {
        try {
            TypeValidator.VALIDATORS.get(paramType).validate(inputValue);
        } catch (NullPointerException | InputValidationException e) {
            throw new EngineCommonException("Input: " + paramName + " type isn't correct", e);
        }
    }

    private static boolean isOutputDependentInputSpecified(String value, Collection<IInputDescriptor> inputs) {
        boolean contains = false;
        for (IInputDescriptor input : inputs) {
            if (value.contains(input.getInputName())) {
                contains = true;
            }
        }
        return contains;
    }

}
