package pt.isel.ngspipes.engine_common.entities.factory;

import pt.isel.ngspipes.engine_common.entities.Environment;
import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.*;
import pt.isel.ngspipes.engine_common.entities.contexts.strategy.*;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.engine_common.utils.DescriptorsUtils;
import pt.isel.ngspipes.engine_common.utils.RepositoryUtils;
import pt.isel.ngspipes.engine_common.utils.SpreadJobExpander;
import pt.isel.ngspipes.engine_common.utils.ValidateUtils;
import pt.isel.ngspipes.pipeline_descriptor.IPipelineDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IPipelineRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IToolRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.IStepDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.CommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.ICommandExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.exec.IPipelineExecDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IChainInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.IParameterInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.input.ISimpleInputDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.ISpreadDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.ICombineStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IInputStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IStrategyDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.tool_descriptor.interfaces.*;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class JobFactory {

    public static List<Job> getJobs(IPipelineDescriptor pipelineDesc, Map<String, Object> parameters,
                                    String workingDirectory, String fileSeparator, Map<String, IToolsRepository> toolsRepos,
                                    Map<String, ICommandDescriptor> cmdDescriptorById) throws EngineCommonException {

        Map<String, IPipelinesRepository> pipelinesRepos = getPipelinesRepositories(pipelineDesc.getRepositories(), parameters);
        Map<String, Map.Entry<Job, ICommandDescriptor>> jobsCmdMap = new HashMap<>();
        addJobs(pipelineDesc, parameters, workingDirectory, toolsRepos, pipelinesRepos, "", jobsCmdMap, fileSeparator, cmdDescriptorById);
        setJobsInOut(pipelineDesc, parameters, toolsRepos, pipelinesRepos, jobsCmdMap);
        return jobsCmdMap.values().stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    public static void expandReadyJobs(List<Job> jobs, Pipeline pipeline, String fileSeparator) throws EngineCommonException {
        List<Job> spreadJobs = new LinkedList<>();
        Map<String, List<Job>> parentsSpread = new HashMap<>();
        for (Job job : jobs) {
            SimpleJob sJob = (SimpleJob) job;
            if (job.isInconclusive())
                continue;
            if (job.getSpread() != null) {
                parentsSpread.put(sJob.getId(), getSpreadInputsParents(pipeline, sJob));
                spreadJobs.add(sJob);
            }
        }
        for (Job spreadJob : spreadJobs) {
            if (parentsSpread.containsKey(spreadJob.getId()) && parentsSpread.get(spreadJob.getId()).isEmpty())
                SpreadJobExpander.expandJobs(pipeline, (SimpleJob) spreadJob, null, fileSeparator);
        }
    }

    public static Environment getJobEnvironment(String id, String workingDirectory, String fileSeparator) {
        String stepWorkDir = workingDirectory + fileSeparator + id;
        Environment environment = new Environment();
        environment.setOutputsDirectory(stepWorkDir);
        environment.setWorkDirectory(stepWorkDir);
        return environment;
    }



    private static List<Job> getSpreadInputsParents(Pipeline pipeline, SimpleJob sJob) {
        List<Job> parentsSpread = new LinkedList<>();
        for (String inName : sJob.getSpread().getInputs()) {
            Input inputById = sJob.getInputById(inName);
            if (inputById.getOriginJob() == null) {
                parentsSpread.add(pipeline.getJobById(inputById.getOriginStep().get(0)));
            }
            if (!inputById.getOriginStep().get(0).equals(sJob.getId())) {
                parentsSpread.add(inputById.getOriginJob().get(0));
            }
        }
        return parentsSpread;
    }

    private static void addJobs(IPipelineDescriptor pipelineDesc, Map<String, Object> parameters,
                                String workingDirectory, Map<String, IToolsRepository> toolsRepos,
                                Map<String, IPipelinesRepository> pipesRepos, String subId,
                                Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap, String fileSeparator,
                                Map<String, ICommandDescriptor> cmdDescriptorById) throws EngineCommonException {
        for (IStepDescriptor step : pipelineDesc.getSteps()) {
            if (step.getExec() instanceof ICommandExecDescriptor) {
                addSimpleJob(parameters, workingDirectory, subId, toolsRepos, step, jobCmdMap, fileSeparator, cmdDescriptorById);
            } else {
                addComposeJob(parameters, workingDirectory, step, pipesRepos, subId, jobCmdMap, pipelineDesc, fileSeparator, cmdDescriptorById);
            }
        }
    }

    private static void addSimpleJob(Map<String, Object> params, String workingDirectory, String subId,
                                     Map<String, IToolsRepository> toolsRepos, IStepDescriptor step,
                                     Map<String, Map.Entry<Job, ICommandDescriptor>> jobsCmdMap,
                                     String fileSeparator, Map<String, ICommandDescriptor> cmdDescriptorById) throws EngineCommonException {
        String stepId = step.getId();
        CommandExecDescriptor exec = (CommandExecDescriptor) step.getExec();
        IToolsRepository repo = toolsRepos.get(exec.getRepositoryId());
        IToolDescriptor tool = DescriptorsUtils.getTool(repo, exec.getToolName(), stepId);
        ICommandDescriptor cmdDesc = DescriptorsUtils.getCommandByName(tool.getCommands(), exec.getCommandName());
        ExecutionContext execCtx = getExecutionContext(params, step, tool);

        assert cmdDesc != null;
        String command = cmdDesc.getCommand();
        String jobId = stepId;
        if (!subId.isEmpty())
            jobId = generateSubJobId(subId, stepId);
        Environment environment = getJobEnvironment(jobId, workingDirectory, fileSeparator);
        cmdDescriptorById.put(jobId, cmdDesc);
        Job job = new SimpleJob(jobId, environment, command, execCtx);
        job.setState(new ExecutionState(StateEnum.STAGING, null));
        addJobSpread(step, job);
        jobsCmdMap.put(jobId, new AbstractMap.SimpleEntry<>(job, cmdDesc));
    }

    private static void addJobSpread(IStepDescriptor step, Job job) {
        ISpreadDescriptor spread = step.getSpread();
        if (spread != null) {
            Collection<String> inputsToSpread = spread.getInputsToSpread();
            ICombineStrategy strategy = getStrategy(spread);
            Spread spreadContext = new Spread(inputsToSpread, strategy);
            job.setSpread(spreadContext);
        }
    }

    private static void addComposeJob(Map<String, Object> params, String workingDirectory, IStepDescriptor step,
                                      Map<String, IPipelinesRepository> pRepos, String subId,
                                      Map<String, Map.Entry<Job, ICommandDescriptor>> jobsCmdMap,
                                      IPipelineDescriptor pipelineDesc, String fileSeparator,
                                      Map<String, ICommandDescriptor> cmdDescriptorById) throws EngineCommonException {
        String stepId = step.getId();
        IPipelineDescriptor pipesDescriptor = DescriptorsUtils.getPipelineDescriptor(pRepos, step);
        Collection<IRepositoryDescriptor> repositories = pipesDescriptor.getRepositories();
        Map<String, IToolsRepository> subToolsRepos = getToolsRepositories(repositories, params);
        Map<String, IPipelinesRepository> subPipesRepos = getPipelinesRepositories(repositories, params);
        updateSubPipelineInputs(pipesDescriptor, step);
        addJobs(pipesDescriptor, params, workingDirectory, subToolsRepos, subPipesRepos, subId + stepId, jobsCmdMap, fileSeparator, cmdDescriptorById);
        updateChainInputsToSubPipeline(pipelineDesc, step, jobsCmdMap, pipesDescriptor);
    }

    private static void updateChainInputsToSubPipeline(IPipelineDescriptor pipeDesc, IStepDescriptor step,
                                                       Map<String, Map.Entry<Job, ICommandDescriptor>> jobsCmdMap, IPipelineDescriptor subPipeDesc) {
        for (IStepDescriptor stepDesc : pipeDesc.getSteps()) {
            if (!stepDesc.getId().equals(step.getId())) {
                for (IInputDescriptor input : stepDesc.getInputs()) {
                    if (input instanceof IChainInputDescriptor) {
                        IChainInputDescriptor in = (IChainInputDescriptor) input;
                        if (in.getStepId().equals(step.getId())) {
                            pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor out = DescriptorsUtils.getOutputFromPipeline(subPipeDesc, in.getOutputName());
                            assert out != null;
                            Job job = getOriginJobByStepId(jobsCmdMap, step.getId() + "_" + out.getStepId());
                            assert job != null;
                            in.setStepId(job.getId());
                            in.setOutputName(out.getOutputName());
                        }
                    }
                }
            }
        }
    }

    private static Job getOriginJobByStepId(Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap, String stepId) {
        if (jobCmdMap.containsKey(stepId))
            return jobCmdMap.get(stepId).getKey();
        else {
            for (Map.Entry<Job, ICommandDescriptor> entry : jobCmdMap.values()) {
                Job job = entry.getKey();
                if (job.getId().contains(stepId))
                    return job;
            }
        }
        return null;
    }

    private static void setJobsInOut(IPipelineDescriptor pipelineDesc, Map<String, Object> parameters,
                                     Map<String, IToolsRepository> toolsRepos,
                                     Map<String, IPipelinesRepository> pipelinesRepos,
                                     Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineCommonException {
        for (Map.Entry<String, Map.Entry<Job, ICommandDescriptor>> entry : jobCmdMap.entrySet()) {
            Job job = entry.getValue().getKey();
            String id = job.getId();
            IPipelineDescriptor pipelineDescriptor = pipelineDesc;
            IStepDescriptor step;
            if (job.getId().contains("_")) {
                String stepId = job.getId();
                id = id.substring(id.lastIndexOf("_") + 1);
                stepId = stepId.substring(0, stepId.indexOf("_"));
                pipelineDescriptor = getPipelineDescriptor(pipelinesRepos, pipelineDescriptor, id, job.getId());
                assert pipelineDescriptor != null;
                step = DescriptorsUtils.getStepById(pipelineDescriptor, id);
                assert step != null;
//                pipelineDescriptor = DescriptorsUtils.getPipelineDescriptor(pipelinesRepos, step);
//                step = DescriptorsUtils.getStepById(pipelineDescriptor, stepId);
            } else
                step = DescriptorsUtils.getStepById(pipelineDesc, id);
            List<Input> inputs = getInputs(parameters, job, jobCmdMap, toolsRepos, pipelinesRepos, pipelineDesc, step);
            job.setInputs(inputs);
            List<Output> outputs = getOutputs(job, jobCmdMap, inputs);
            job.setOutputs(outputs);
        }
    }

    private static void updateSubPipelineInputs(IPipelineDescriptor pipelineDesc, IStepDescriptor step) {
        List<IInputDescriptor> toRemove = new LinkedList<>();
        List<IInputDescriptor> toAdd = new LinkedList<>();
        List<String> visit = new LinkedList<>();

        for (IStepDescriptor subStep : pipelineDesc.getSteps()) {
            for (IInputDescriptor input : subStep.getInputs()) {
                String inputName = input.getInputName();
                if (input instanceof IParameterInputDescriptor && !visit.contains(inputName)) {
                    visit.add(inputName);
                    String parameterName = ((IParameterInputDescriptor) input).getParameterName();
                    IInputDescriptor in = DescriptorsUtils.getInputByName(step.getInputs(), parameterName);
                    assert in != null;
                    in.setInputName(inputName);
                    toAdd.add(in);
                    toRemove.add(input);
                } else if (input instanceof IChainInputDescriptor) {
                    IChainInputDescriptor in = (IChainInputDescriptor) input;
                    if (DescriptorsUtils.getStepById(pipelineDesc, in.getStepId()) != null)
                        in.setStepId(step.getId() + "_" + in.getStepId());
                }
            }
            for (IInputDescriptor in : toRemove) {
                subStep.getInputs().remove(in);
            }
            for (IInputDescriptor in : toAdd) {
                subStep.getInputs().add(in);
            }
        }
    }


    private static ExecutionContext getExecutionContext(Map<String, Object> parameters, IStepDescriptor step, IToolDescriptor tool) {
        IExecutionContextDescriptor execCtx = DescriptorsUtils.getExecutionContext(tool.getExecutionContexts(), step.getExecutionContext(), parameters);
        assert execCtx != null;
        return new ExecutionContext(execCtx.getName(), execCtx.getContext(), execCtx.getConfig());
    }

    private static List<Output> getOutputs(Job job, Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap,
                                           List<Input> inputs) throws EngineCommonException {
        List<Output> outputs = new LinkedList<>();
        List<Output> outputsToRemove = new LinkedList<>();
        List<String> visitOutputs = new LinkedList<>();
        Map<String, List<String>> usedBy = new HashMap<>();
        Map<String, IOutputDescriptor> outputsByName = new HashMap<>();

        for (IOutputDescriptor output : jobCmdMap.get(job.getId()).getValue().getOutputs()) {
            outputsByName.put(output.getName(), output);
            addOutput(job, jobCmdMap, inputs, outputs, visitOutputs, usedBy, output);
        }
        setUsedBy(outputs, usedBy);
        visitOutputs = new LinkedList<>();
        for (IOutputDescriptor output : jobCmdMap.get(job.getId()).getValue().getOutputs()) {
           setNotRequireOutputs(output, job.getInputs(), usedBy, outputsToRemove, outputs, visitOutputs, outputsByName);
        }

        outputs.removeAll(outputsToRemove);

        return outputs;
    }

    private static List<Input> getInputs(Map<String, Object> params, Job job,
                                         Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap,
                                         Map<String, IToolsRepository> toolsRepos,
                                         Map<String, IPipelinesRepository> pipeRepos,
                                         IPipelineDescriptor pipelinesDesc, IStepDescriptor step)
                                        throws EngineCommonException {

        String jobId = job.getId();
        List<Input> inputs = new LinkedList<>();
        List<String> visitParams = new LinkedList<>();
        for (IParameterDescriptor param : jobCmdMap.get(jobId).getValue().getParameters()) {
            String name = param.getName();
            if (!visitParams.contains(name)) {
                addInput(params, step, toolsRepos, pipeRepos, pipelinesDesc, inputs, visitParams, param, name, job, jobCmdMap);
            }
        }
        return inputs;
    }

    private static void addInput(Map<String, Object> params, IStepDescriptor step, Map<String, IToolsRepository> toolsRepos,
                                 Map<String, IPipelinesRepository> pipeRepos, IPipelineDescriptor pipeDesc,
                                 List<Input> inputs, List<String> visitParams, IParameterDescriptor param,
                                 String name, Job job, Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineCommonException {
        visitParams.add(name);
        IInputDescriptor input = DescriptorsUtils.getInputByName(step.getInputs(), name);
        ValidateUtils.validateInput(step, jobCmdMap.get(job.getId()).getValue(), param, input, pipeRepos, pipeDesc, params);
        StringBuilder inputValue = new StringBuilder();
        List<IParameterDescriptor> subParams = (List<IParameterDescriptor>) param.getSubParameters();
        if (input == null) {
            if (subParams != null && !subParams.isEmpty()) {
                List<Input> subInputs = new LinkedList<>();
                addSubInputs(params, step, toolsRepos, pipeRepos, pipeDesc, visitParams, subParams, subInputs, jobCmdMap);
                if (!subInputs.isEmpty()) {
                    LinkedList<Job> originJob = new LinkedList<>(Collections.singletonList(job));
                    Input in = new Input(param.getName(), originJob, "",param.getType(), "",
                            getFix(param.getPrefix()), getFix(param.getSeparator()),
                            getFix(param.getSuffix()), subInputs);
                    inputs.add(in);
                }
            }

        } else {
            addSimpleInput(params, step, toolsRepos, pipeRepos, pipeDesc, inputs, param, name, input, inputValue, jobCmdMap);
        }
    }

    private static void addSimpleInput(Map<String, Object> params, IStepDescriptor step, Map<String, IToolsRepository> tRepos,
                                       Map<String, IPipelinesRepository> pRepos, IPipelineDescriptor pipeDesc,
                                       List<Input> inputs, IParameterDescriptor param, String name, IInputDescriptor input,
                                       StringBuilder inputValue,
                                       Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineCommonException {
        Map.Entry<String, Map.Entry<String, String>> inVal;
        Map.Entry<String, String> inMap = null;
        String stepId, outName = "";
        inVal = getInputValue(params, tRepos, pRepos, pipeDesc, input);
        if (!inVal.getValue().getKey().isEmpty())
            inMap = inVal.getValue();
        inputValue.append(inVal.getKey());

        if (inMap != null) {
            String key = inMap.getKey();
            if (key.isEmpty()) {
                if (!(input instanceof IChainInputDescriptor)) {
                    stepId = step.getId();
                } else {
                    stepId = ((IChainInputDescriptor) input).getStepId();
                }
            } else {
                stepId = key;
                outName = inMap.getValue();
            }
        } else {
            stepId = step.getId();
        }

        Job originJob = getOriginJobByStepId(jobCmdMap, stepId);
        assert originJob != null;
        LinkedList<Job> originJobs = new LinkedList<>(Collections.singletonList(originJob));
        Input inputContext = new Input(name, originJobs, outName, param.getType(),
                inputValue.toString(), getFix(param.getPrefix()), getFix(param.getSeparator()),
                getFix(param.getSuffix()), new LinkedList<>());
        inputs.add(inputContext);
    }

    private static void addSubInputs(Map<String, Object> params, IStepDescriptor step, Map<String, IToolsRepository> toolsRepos,
                                     Map<String, IPipelinesRepository> pipeRepos, IPipelineDescriptor pipeDesc,
                                     List<String> visitParams, List<IParameterDescriptor> subParams,
                                     List<Input> subInputs, Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap) throws EngineCommonException {
        Map.Entry<String, Map.Entry<String, String>> inVal;
        String outName = "", stepId;
        for (IParameterDescriptor subParam : subParams) {
            visitParams.add(subParam.getName());
            IInputDescriptor in = DescriptorsUtils.getInputByName(step.getInputs(), subParam.getName());
            if (in != null) {
                inVal = getInputValue(params, toolsRepos, pipeRepos, pipeDesc, in);
                if (!inVal.getValue().getKey().isEmpty()) {
                    stepId = inVal.getValue().getKey();
                    outName = inVal.getValue().getValue();
                } else {
                    stepId = step.getId();
                }


                List<Input> subIns = new LinkedList<>();
                LinkedList<Job> originJobs = new LinkedList<>(Collections.singletonList(jobCmdMap.get(stepId).getKey()));
                Input inputContext = new Input(in.getInputName(), originJobs, outName, subParam.getType(),
                                                inVal.getKey(), getFix(subParam.getPrefix()),
                                                getFix(subParam.getSeparator()), getFix(subParam.getSuffix()), subIns);
                subInputs.add(inputContext);
            }
        }

    }

    private static String getFix(String prefix) {
        return prefix == null ? "" : prefix;
    }

    private static Map.Entry<String, Map.Entry<String, String>> getInputValue(Map<String, Object> params,
                                                                              Map<String, IToolsRepository> toolsRepos,
                                                                              Map<String, IPipelinesRepository> pipesRepos,
                                                                              IPipelineDescriptor pipeDesc,
                                                                              IInputDescriptor input) throws EngineCommonException {
        StringBuilder value = new StringBuilder();
        String stepId = "";
        String outName = "";

        if (input instanceof IParameterInputDescriptor) {
            value.append(getParameterInputValue(input, params));
        } else if (input instanceof ISimpleInputDescriptor) {
            value.append(getSimpleInputValue((ISimpleInputDescriptor) input));
        } else if (input instanceof IChainInputDescriptor) {
            IChainInputDescriptor input1 = (IChainInputDescriptor) input;
            outName = input1.getOutputName();
            value.append(getChainInputValue(params, toolsRepos, pipesRepos, pipeDesc, input1));
            stepId = input1.getStepId();
        }
        return new AbstractMap.SimpleEntry<>(value.toString(), new AbstractMap.SimpleEntry<>(stepId, outName));
    }

    private static String getChainInputValue(Map<String, Object> parameters, Map<String, IToolsRepository> toolsRepos,
                                             Map<String, IPipelinesRepository> pipelinesRepos,
                                             IPipelineDescriptor pipelineDesc,
                                             IChainInputDescriptor input) throws EngineCommonException {
        Object value;
        String dependentStep = input.getStepId();
        IPipelineDescriptor pipelineDescriptor = pipelineDesc;
        if (dependentStep.contains("_")) {
            String baseStep = dependentStep.substring(0, dependentStep.lastIndexOf("_"));
            dependentStep = getStepName(dependentStep, baseStep, pipelineDesc);
            pipelineDescriptor = getPipelineDescriptor(pipelinesRepos, pipelineDesc, baseStep, input.getStepId());
        }

        assert pipelineDescriptor != null;
        IStepDescriptor stepDesc = DescriptorsUtils.getStepById(pipelineDescriptor, dependentStep);
        String outName = input.getOutputName();
        assert stepDesc != null;
        if (toolsRepos.containsKey(stepDesc.getExec().getRepositoryId())) {
            StringBuilder val = getOutputValue(parameters, toolsRepos, pipelinesRepos, pipelineDesc, outName, stepDesc);
            value = val.toString();
        } else {
            IPipelinesRepository pipelinesRepository = pipelinesRepos.get(stepDesc.getExec().getRepositoryId());
            IPipelineExecDescriptor pipeExec = (IPipelineExecDescriptor) stepDesc.getExec();
            pipelineDescriptor = DescriptorsUtils.getPipelineDescriptor(pipelinesRepository, pipeExec.getPipelineName());
            pt.isel.ngspipes.pipeline_descriptor.output.IOutputDescriptor output;
            output = DescriptorsUtils.getOutputFromPipeline(pipelineDescriptor, outName);
            assert output != null;
            IStepDescriptor subStep = DescriptorsUtils.getStepById(pipelineDescriptor, output.getStepId());
            assert subStep != null;
            Map<String, IToolsRepository> subToolsRepos = getToolsRepositories(pipelineDescriptor.getRepositories(), parameters);
            Map<String, IPipelinesRepository> subPipelinesRepos = getPipelinesRepositories(pipelineDescriptor.getRepositories(), parameters);
            StringBuilder val = getOutputValue(parameters, subToolsRepos, subPipelinesRepos, pipelineDescriptor, output.getOutputName(), subStep);
            value = val.toString();
        }
        return value.toString();
    }

    private static IPipelineDescriptor getPipelineDescriptor(Map<String, IPipelinesRepository> pipelinesRepos,
                                                             IPipelineDescriptor parentPipe, String subStep, String stepId) throws EngineCommonException {
        for (IStepDescriptor step : parentPipe.getSteps()) {
            String pipelineName = "";
            if (step.getId().equals(subStep) && step.getExec() instanceof ICommandExecDescriptor) {
                return parentPipe;
            } else if (stepId.contains(step.getId())) {
                if (step.getExec() instanceof ICommandExecDescriptor) {
                    stepId = stepId.replace(subStep + "_", "");
                    if (step.getId().equals(stepId))
                        return parentPipe;
                } else if (step.getExec() instanceof IPipelineExecDescriptor) {
                    pipelineName = step.getId();
                }
                IPipelinesRepository pipelinesRepository = pipelinesRepos.get(step.getExec().getRepositoryId());
                IPipelineDescriptor pipelineDescriptor = DescriptorsUtils.getPipelineDescriptor(pipelinesRepository, pipelineName);
                return getPipelineDescriptor(pipelinesRepos, pipelineDescriptor, subStep, stepId);
            }
        }
        return null;
    }

    private static String getStepName(String dependentStep, String baseStep, IPipelineDescriptor pipelineDesc) {
        int lastIdx = dependentStep.lastIndexOf("_");
        if (DescriptorsUtils.getStepById(pipelineDesc, baseStep).getExec() instanceof IPipelineExecDescriptor)
            return dependentStep.substring(lastIdx + 1);
        return baseStep;
    }

    private static StringBuilder getOutputValue(Map<String, Object> params, Map<String, IToolsRepository> toolsRepos,
                                                Map<String, IPipelinesRepository> pipelinesRepos, IPipelineDescriptor pipelineDesc,
                                                String outputName, IStepDescriptor stepDesc) throws EngineCommonException {
        assert stepDesc != null;
        CommandExecDescriptor exec = (CommandExecDescriptor) stepDesc.getExec();
        IToolsRepository repo = toolsRepos.get(exec.getRepositoryId());
        IToolDescriptor tool = DescriptorsUtils.getTool(repo, exec.getToolName(), exec.getCommandName());
        ICommandDescriptor command = DescriptorsUtils.getCommandByName(tool.getCommands(), exec.getCommandName());
        assert command != null;
        IOutputDescriptor output = DescriptorsUtils.getOutputFromCommand(command, outputName);
        assert output != null;
        String outputValue = output.getValue();
        StringBuilder val = new StringBuilder();
        if (outputValue.contains("$")) {
            if (outputValue.indexOf("$") != outputValue.lastIndexOf("$")) {
                String[] splittedByDependency = outputValue.split("$");
                for (String str : splittedByDependency) {
                    IInputDescriptor dependentInput = DescriptorsUtils.getInputByName(stepDesc.getInputs(), str);
                    val.append(getInputValue(params, toolsRepos, pipelinesRepos, pipelineDesc, dependentInput));
                }
            } else {
                String currValue = outputValue.substring(outputValue.indexOf("$") + 1);
                int idxLast = currValue.contains("/") ? currValue.indexOf("/") :
                                currValue.contains("\\") ? currValue.indexOf("\\") :
                                        currValue.contains(".*") ? currValue.indexOf(".*") : -1;
                String inName = currValue;
                if (idxLast != -1)
                    inName = currValue.substring(0, idxLast);
                IInputDescriptor dependentInput = DescriptorsUtils.getInputByName(stepDesc.getInputs(), inName);
                String inVal = getInputValue(params, toolsRepos, pipelinesRepos, pipelineDesc, dependentInput).getKey();
                val.append(currValue.replace(inName, inVal));
            }
        } else {
            val.append(outputValue);
        }
        return val;
    }

    private static String getSimpleInputValue(ISimpleInputDescriptor inputDescriptor) {
        Object inputValue = inputDescriptor.getValue();
        if (inputValue == null)
            inputValue = "";
        return inputValue.toString();
    }

    private static Object getParameterInputValue(IInputDescriptor inputDescriptor, Map<String, Object> parameters) {
        Object inputValue = parameters.get(((IParameterInputDescriptor) inputDescriptor).getParameterName());
        if (inputValue == null) {
            inputValue = "";
        }
        return inputValue;
    }

    private static void addOutput(Job job, Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap,
                                  List<Input> inputs, List<Output> outputs, List<String> visitOutputs,
                                  Map<String, List<String>> usedBy, IOutputDescriptor output) throws EngineCommonException {
        String name = output.getName();
        if (!visitOutputs.contains(name)) {
            String outputValue = output.getValue();
            String type = output.getType();
            String value = getOutputValue(inputs, outputs, output.getName(), outputValue, jobCmdMap, job, usedBy, type);
            Output outContext = new Output(output.getName(), job, type, value);
            outputs.add(outContext);
            visitOutputs.add(name);
        }
    }

    private static void setNotRequireOutputs(IOutputDescriptor output, List<Input> inputs, Map<String, List<String>> usedBy,
                                             List<Output> toRemove, List<Output> outputs, List<String> visitOutputs,
                                             Map<String, IOutputDescriptor> outputsByName) {
        String value = output.getValue();
        if (value.contains("$")) {
            List<Input> orderdInputs = getOrderInputsByNameLength(inputs);
            if (value.lastIndexOf("$") == value.indexOf("$")) {
                String outValue = value.substring(value.indexOf("$") + 1);
                setNotRequireOutput(output, outputs, usedBy, toRemove, orderdInputs, outValue, visitOutputs, outputsByName);
            } else {
                String[] values = value.split("\\$");
                for (String outValue : values) {
                    if (!outValue.isEmpty())
                        setNotRequireOutput(output, outputs, usedBy, toRemove, orderdInputs, outValue, visitOutputs, outputsByName);
                }
            }
        }
        visitOutputs.add(output.getName());
    }

    private static void setNotRequireOutput(IOutputDescriptor output, List<Output> outputs,
                                            Map<String, List<String>> usedBy, List<Output> toRemove,
                                            List<Input> inputs, String outValue, List<String> visitOutputs,
                                            Map<String, IOutputDescriptor> outputsByName) {
        if (outValue.contains("/"))
            outValue = outValue.substring(0, outValue.indexOf("/"));
        if (outValue.contains("*")) {
            int idx = outValue.indexOf("*");
            if (idx == 0)
                outValue = outValue.substring(1);
            else
                outValue = outValue.substring(0, idx);
        }
        if (outValue.contains(".")) {
            int idx = outValue.indexOf(".");
            if (idx == 0)
                outValue = outValue.substring(1);
            else
                outValue = outValue.substring(0, idx);
        }
        for (String visitOutput : visitOutputs) {
            if (output.getName().equals(visitOutput))
                return;
        }

        if(!isDependentInputDefined(outValue, inputs)) {
            if (usedBy.isEmpty()) {
                for (Output out : outputs) {
                    if (out.getName().equals(output.getName())) {
                        toRemove.add(out);
                        return;
                    }
                }
            } else {
                List<String> keys = new ArrayList<>(usedBy.keySet());
                keys.sort((i0, i1) -> Integer.compare(i1.length(), i0.length()));
                keys.stream()
                    .filter(outValue::contains)
                    .findFirst()
                    .ifPresent(s -> setNotRequireOutputs(outputsByName.get(s), inputs, usedBy, toRemove, outputs,
                                                        visitOutputs, outputsByName));
            }
        }
    }

    private static boolean isDependentInputDefined(String outputValue, List<Input> inputs) {
        for (Input input : inputs)
            if (outputValue.equals(input.getName()))
                return true;
        return false;
    }


    private static String getOutputValue(List<Input> inputs, List<Output> outputs, String outputName,
                                         String outputValue, Map<String, Map.Entry<Job, ICommandDescriptor>> jobCmdMap,
                                         Job job, Map<String, List<String>> usedBy, String type) throws EngineCommonException {
        StringBuilder value = new StringBuilder();
        if (outputValue.contains("$")) {
            if (outputValue.indexOf("$") != outputValue.lastIndexOf("$")) {
                String[] splittedByDependency = outputValue.split("\\$");
                for (String val : splittedByDependency) {
                    if (!val.isEmpty())
                    dependentOutValue(inputs, outputs, jobCmdMap, job, value, val, usedBy, outputName, type);
                }
            } else {
                int idx = outputValue.indexOf("$") + 1;
                String val = outputValue.substring(idx);
                dependentOutValue(inputs, outputs, jobCmdMap, job, value, val, usedBy, outputName, type);
            }
        } else {
            value.append(outputValue);
        }
        return value.toString();
    }

    private static void dependentOutValue(List<Input> inputs, List<Output> outputs, Map<String, Map.Entry<Job,
                                          ICommandDescriptor>> jobCmdMap, Job job, StringBuilder value,
                                          String outName, Map<String, List<String>> usedBy,
                                          String outputName, String type) throws EngineCommonException {
        int idxLast = outName.indexOf(File.separatorChar);
        String val = outName;
        if (idxLast != -1)
            val = outName.substring(0, idxLast);

        Collection<IOutputDescriptor> cmdOutputs = jobCmdMap.get(job.getId()).getValue().getOutputs();
        List<IOutputDescriptor> orderedOutputs = getOutputsOrderedByNameLength(cmdOutputs);
        IOutputDescriptor output = null;
        for (IOutputDescriptor outputDescriptor : orderedOutputs) {
            if (val.contains(outputDescriptor.getName()) && !outputDescriptor.getName().equalsIgnoreCase(outputName)) {
                output = outputDescriptor;
                break;
            }
        }

        if (output != null) {
            String outputVal = getOutputValue(inputs, outputs, output.getName(), output.getValue(), jobCmdMap, job, usedBy, type);
            outputVal = outName.replace(output.getName(), outputVal);

            if (!usedBy.containsKey(output.getName()))
                usedBy.put(output.getName(), new LinkedList<>());
            if (!usedBy.get(output.getName()).contains(outputName))
                usedBy.get(output.getName()).add(outputName);
            value.append(outputVal);
        } else {
            getOutputDependentValue(inputs, value, outName);
        }
    }

    private static List<IOutputDescriptor> getOutputsOrderedByNameLength(Collection<IOutputDescriptor> outputs) {
        List<IOutputDescriptor> orderedOutputs = new LinkedList<>(outputs);

        Comparator<IOutputDescriptor> outputLengthComparator = (i0, i1) -> Integer.compare(i1.getName().length(), i0.getName().length());
        orderedOutputs.sort(outputLengthComparator);

        return orderedOutputs;
    }

    private static void getOutputDependentValue(List<Input> inputs, StringBuilder value, String outName) {
        List<Input> orderedInputs = getOrderInputsByNameLength(inputs);
        for (Input input : orderedInputs) {
            if (outName.contains(input.getName())) {
                String outVal = outName.replace(input.getName(), input.getValue());
                value.append(outVal);
                return;
            }
        }
    }

    private static List<Input> getOrderInputsByNameLength(List<Input> inputs) {
        LinkedList<Input> orderedInputs = new LinkedList<>(inputs);
        Comparator<Input> inputLengthComparator = (i0, i1) -> Integer.compare(i1.getName().length(), i0.getName().length());

        orderedInputs.sort(inputLengthComparator);
        return orderedInputs;
    }

    private static void setUsedBy(List<Output> outputs, Map<String, List<String>> usedBy) {
        for (Output out : outputs) {
            if (usedBy.containsKey(out.getName()))
                out.setUsedBy(usedBy.get(out.getName()));
        }
    }

    private static String generateSubJobId(String subId, String stepId) {
        return subId + "_" + stepId;
    }

    private static ICombineStrategy getStrategy(ISpreadDescriptor spread) {
        ICombineStrategyDescriptor strategy = spread.getStrategy();
        if (strategy != null)
            return getCombineStrategy(strategy);
        return null;
    }

    private static ICombineStrategy getCombineStrategy(ICombineStrategyDescriptor strategy) {

        IStrategyDescriptor first = strategy.getFirstStrategy();
        IStrategyDescriptor second = strategy.getSecondStrategy();

        if (first instanceof IInputStrategyDescriptor) {
            if (second instanceof IInputStrategyDescriptor) {
                InputStrategy in = new InputStrategy(((IInputStrategyDescriptor) first).getInputName());
                InputStrategy in1 = new InputStrategy(((IInputStrategyDescriptor) second).getInputName());
                return new OneToOneStrategy(in, in1);
            } else {
                InputStrategy in = new InputStrategy(((IInputStrategyDescriptor) first).getInputName());
                IStrategy strategy1 = getCombineStrategy((ICombineStrategyDescriptor) second);
                return new OneToManyStrategy(in, strategy1);
            }
        } else if (second instanceof IInputStrategyDescriptor) {
            InputStrategy in = new InputStrategy(((IInputStrategyDescriptor) second).getInputName());
            IStrategy strategy1 = getCombineStrategy((ICombineStrategyDescriptor) first);
            return new OneToManyStrategy(strategy1, in);
        } else {
            IStrategy strategy1 = getCombineStrategy((ICombineStrategyDescriptor) first);
            IStrategy strategy2 = getCombineStrategy((ICombineStrategyDescriptor) second);
            return new OneToManyStrategy(strategy1, strategy2);
        }

    }

    private static Map<String, IPipelinesRepository> getPipelinesRepositories(Collection<IRepositoryDescriptor> repositories,
                                                                              Map<String, Object> parameters) throws EngineCommonException {
        Map<String, IPipelinesRepository> pipelinesRepositoryMap = new HashMap<>();

        for (IRepositoryDescriptor repo : repositories) {
            if(pipelinesRepositoryMap.containsKey(repo.getId()))
                throw new EngineCommonException("Repositories ids can be duplicated. Id: " + repo.getId() + " is duplicated.");
            if (repo instanceof IPipelineRepositoryDescriptor) {
                IPipelineRepositoryDescriptor pRepoDesc = (IPipelineRepositoryDescriptor) repo;
                IPipelinesRepository pipelinesRepo = RepositoryUtils.getPipelinesRepository(pRepoDesc, parameters);
                pipelinesRepositoryMap.put(repo.getId(), pipelinesRepo);
            }
        }

        return pipelinesRepositoryMap;
    }

    private static Map<String,IToolsRepository> getToolsRepositories(Collection<IRepositoryDescriptor> repositories,
                                                                     Map<String, Object> parameters) throws EngineCommonException {
        Map<String, IToolsRepository> toolsRepositoryMap = new HashMap<>();

        for (IRepositoryDescriptor repo : repositories) {
            if(toolsRepositoryMap.containsKey(repo.getId()))
                throw new EngineCommonException("Repositories ids can be duplicated. Id: " + repo.getId() + " is duplicated.");
            if (repo instanceof IToolRepositoryDescriptor) {
                IToolRepositoryDescriptor toolsRepoDesc = (IToolRepositoryDescriptor) repo;
                IToolsRepository toolsRepo = RepositoryUtils.getToolsRepository(toolsRepoDesc, parameters);
                toolsRepositoryMap.put(repo.getId(), toolsRepo);
            }
        }

        return toolsRepositoryMap;
    }


}
