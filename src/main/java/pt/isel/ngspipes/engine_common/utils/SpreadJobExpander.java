package pt.isel.ngspipes.engine_common.utils;

import pt.isel.ngspipes.engine_common.entities.Environment;
import pt.isel.ngspipes.engine_common.entities.ExecutionState;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.*;
import pt.isel.ngspipes.engine_common.entities.factory.JobFactory;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.function.BiFunction;

public class SpreadJobExpander {

    static class InOutput {
        Input input;
        BiFunction<String, Job, List<String>> getOutputValues;

        InOutput(Input input, BiFunction<String, Job, List<String>> getOutputValues) {
            this.input = input;
            this.getOutputValues = getOutputValues;
        }
    }


    public static void expandJobs(Pipeline pipeline, SimpleJob sJob, BiFunction<String, Job, List<String>> getOutValues, String fileSeparator) throws EngineCommonException {
        List<Job> toRemove = new LinkedList<>();
        List<Job> expandedJobs = getExpandedJobs(pipeline,sJob, toRemove, getOutValues, fileSeparator);
        pipeline.addJobs(expandedJobs);
        pipeline.removeJobs(toRemove);
    }

    public static List<Job> getExpandedJobs(Pipeline pipeline, SimpleJob job, List<Job> toRemove,
                                            BiFunction<String, Job, List<String>> getOutValues,
                                            String fileSeparator) throws EngineCommonException {
        List<Job> spreadJobs = new LinkedList<>();
        addSpreadJobs(job, spreadJobs, pipeline, getOutValues, fileSeparator);
        toRemove.add(job);
        expandChilds(pipeline, job, toRemove, fileSeparator, spreadJobs);
        updatePipelineOutputs(pipeline, toRemove, spreadJobs);
        return spreadJobs;
    }

    public static List<String> getValues(String inputValue) {
        String suffix = "";
        int suffixIdx = inputValue.indexOf("]") - 1;
        inputValue = inputValue.replace("[", "");
        inputValue = inputValue.replace("]", "");

        if (suffixIdx >= 0) {
            suffix = inputValue.length() == suffixIdx ? "" : inputValue.substring(suffixIdx);
        }

        String[] split = inputValue.split(",");
        List<String> inputsValues = new LinkedList<>();

        for (String str : split) {
            String value = str.trim();
            if (!value.contains(suffix))
                value = value + suffix;
            inputsValues.add(value);
        }

        return inputsValues;
    }



    private static void updatePipelineOutputs(Pipeline pipeline, List<Job> toRemove, List<Job> spreadJobs) {
        List<Output> outputsToAdd = new LinkedList<>();
        List<Output> outputsToRemove = new LinkedList<>();
        for (Job r : toRemove) {
            for (Output out : pipeline.getOutputs()) {
                if (out.getOriginJob().equals(r.getId())) {
                    outputsToRemove.add(out);
                    spreadJobs.forEach((j) -> {
                        if (j.getId().contains(r.getId()))
                            outputsToAdd.add(j.getOutputById(out.getName()));
                    });
                }
            }
        }

        pipeline.getOutputs().removeAll(outputsToRemove);
        pipeline.getOutputs().addAll(outputsToAdd);
    }

    private static void expandChilds(Pipeline pipeline, SimpleJob job, List<Job> toRemove, String fileSeparator, List<Job> spreadJobs) throws EngineCommonException {
        expandSpreadChilds(pipeline, job, toRemove, fileSeparator, spreadJobs);
        expandJoinChilds(pipeline, job, fileSeparator, spreadJobs);
    }

    private static void expandJoinChilds(Pipeline pipeline, SimpleJob job, String fileSeparator, List<Job> spreadJobs) {
        for (Job currJob : pipeline.getJobs()) {
            if (currJob.equals(job) || currJob.getState().getState().equals(StateEnum.SUCCESS))
                continue;
            List<Input> chainInputs = getChainInputs(currJob);
            List<Input> spreadChildInputs = getSpreadChildInputs(chainInputs, job);
            if (!spreadChildInputs.isEmpty()) {
                if (isJoinJob(job, currJob, spreadChildInputs)) {
                    expandJoinChild(job, spreadJobs, currJob, chainInputs, fileSeparator);
                    spreadJobs.add(currJob);
                }
            }
        }
    }

    private static void expandJoinChild(SimpleJob job, List<Job> spreadJobs, Job currJob, List<Input> chainInputs, String fileSeparator) {
        chainInputs.forEach((input) -> {
            if (input.getOriginStep().get(0).equals(job.getId())) {
                List<Job> originJobs = new LinkedList<>();
                spreadJobs.forEach((spreadJob) -> {
                    if (spreadJob.getId().contains(job.getId()))
                        originJobs.add(spreadJob);
                });
                input.setOriginJob(originJobs);
                updateInputValues(input, originJobs, fileSeparator);
                updateChainsFrom(job, currJob, originJobs);
            }
        });
    }

    private static void updateChainsFrom(SimpleJob job, Job currJob, List<Job> originJobs) {
        currJob.getChainsFrom().remove(job);
        currJob.getChainsFrom().addAll(originJobs);
    }

    private static void updateInputValues(Input input, List<Job> originJobs, String fileSeparator) {
        StringBuilder inValues = new StringBuilder("[");
        for (Job originJob : originJobs) {
            Output outputById = originJob.getOutputById(input.getChainOutput());
            inValues.append(originJob.getId()).append(fileSeparator).append(outputById.getValue()).append(",");
        }
        inValues.deleteCharAt(inValues.length() - 1);
        inValues.append("]");
        input.setValue(inValues.toString());
    }

    private static void expandSpreadChilds(Pipeline pipeline, SimpleJob job, List<Job> toRemove, String fileSeparator, List<Job> spreadJobs) throws EngineCommonException {
        List<Job> spreadChildJobs = getSpreadChilds(job, pipeline);
        toRemove.addAll(spreadChildJobs);
        for (Job spreadChild : spreadChildJobs) {
            List<Job> expandedChildJobs = new LinkedList<>();
            addSpreadJobs(spreadChild, expandedChildJobs, pipeline, SpreadJobExpander::getMiddleOutputValues, fileSeparator);
            for (int index = 0; index < expandedChildJobs.size(); index++) {
                updateSpreadInputsChain(expandedChildJobs.get(index), spreadChild, job, spreadJobs.get(index));
            }
            spreadJobs.addAll(expandedChildJobs);
        }
    }

    private static void addSpreadJobs(Job job, List<Job> jobs, Pipeline pipeline,
                                      BiFunction<String, Job, List<String>> getOutValues,
                                      String fileSeparator) throws EngineCommonException {
        Spread spread = job.getSpread();
        BiFunction<InOutput, Pipeline, List<String>> values;

        if (isSpreadChain(job)) {
            values = SpreadJobExpander::getMiddleInputValues;
        } else {
            values = SpreadJobExpander::getInitInputValues;
        }

        Map<String, List<String>> valuesOfInputsToSpread = getInputValuesToSpread(job, pipeline, values, getOutValues);
        if (spread.getStrategy() != null)
            SpreadCombiner.getInputsCombination(spread.getStrategy(), valuesOfInputsToSpread);

        spreadByInputs(job, jobs, pipeline, fileSeparator, valuesOfInputsToSpread);
    }

    private static boolean isResourceType(String type) {
        boolean isFile = type.contains("ile");
        boolean directory = type.contains("irectory");
        return isFile || directory;
    }

    private static void updateSpreadInputsChain(Job job, Job baseJob, Job parent, Job parentExpanded) {
        for (String inputToSpread : baseJob.getSpread().getInputs()) {
            Input input = job.getInputById(inputToSpread);
            if (input.getOriginJob().get(0).equals(parent))
                input.updateOriginJob(parentExpanded);
        }
    }

    private static List<String> getMiddleOutputValues(String outputName, Job originJob) {
        String value = originJob.getOutputById(outputName).getValue().toString();
        List<String> values = new LinkedList<>();
        values.add(value);
        return values;
    }

    private static List<Job> getSpreadChilds(SimpleJob job, Pipeline pipeline) {
        List<Job> spreadChilds = new LinkedList<>();
        for (Job currJob : pipeline.getJobs()) {
            if (currJob.equals(job) || currJob.getState().getState().equals(StateEnum.SUCCESS))
                continue;
            List<Input> chainInputs = getChainInputs(currJob);
            List<Input> spreadChildInputs = getSpreadChildInputs(chainInputs, job);
            if (!spreadChildInputs.isEmpty()) {
                if (!isJoinJob(job, currJob, spreadChildInputs)) {
                    spreadChilds.add(currJob);
                }
            }
        }
        return spreadChilds;
    }

    private static boolean isJoinJob(Job parent, Job job, List<Input> chainInputs) {

        if (job.getSpread() == null) {
            return true;
        }

        boolean join = true;
        for (Input chainInput : chainInputs) {
            String inType = chainInput.getType();
            Output output = parent.getOutputById(chainInput.getChainOutput());
            String outType = output.getType();
            if (inType.equalsIgnoreCase(outType)) {
                join = false;
            } else {
                return true;
            }
        }

        return join;
    }

    private static List<Input> getSpreadChildInputs(List<Input> chainInputs, SimpleJob job) {
        List<Input> spreadChildInputs = new LinkedList<>();
        for (Input chainInput : chainInputs) {
            if (chainInput.getOriginStep().get(0).equals(job.getId()))
                spreadChildInputs.add(chainInput);
        }
        return spreadChildInputs;
    }

    private static List<Input> getChainInputs(Job job) {
        List<Input> chainInputs = new LinkedList<>();

        for (Input input: job.getInputs()) {
            if (!input.getOriginStep().isEmpty() && !input.getOriginStep().get(0).equals(job.getId())) {
                chainInputs.add(input);
            }
        }

        return chainInputs;
    }

    private static void spreadByInputs(Job job, List<Job> jobs, Pipeline pipeline, String fileSeparator, Map<String, List<String>> valuesOfInputsToSpread) {
        int idx = 0;
        int len = getInputValuesLength(valuesOfInputsToSpread);

        while (idx < len) {
            Job jobToSpread = getSpreadJob(job, valuesOfInputsToSpread, idx, pipeline, fileSeparator);
            jobs.add(jobToSpread);
            idx++;
        }
    }

    private static List<String> getMiddleInputValues(InOutput inOut, Pipeline pipeline) {
        Input input = inOut.input;
        String chainOutput = input.getChainOutput();
        if (chainOutput != null && !chainOutput.isEmpty() && isResourceType(input.getType())) {
            Job originJob = input.getOriginJob() == null ? pipeline.getJobById(input.getOriginStep().get(0)) : input.getOriginJob().get(0);
            return inOut.getOutputValues.apply(chainOutput, originJob);
        } else
            return getInitInputValues(inOut, pipeline);
    }

    private static List<String> getInitInputValues(InOutput inOut, Pipeline pipeline) {
        String inputValue = inOut.input.getValue();
        return getValues(inputValue);
    }

    private static List<Output> getCopyOfJobOutputs(List<Output> outputs, SimpleJob job, int idx) {
        List<Output> outs = new LinkedList<>();
        for (Output output : outputs) {
//            estou a ignorar que pode vir .* no fim [].*
            List<String> values = getValues(output.getValue().toString());
            String value = values.size() == 1 ? values.get(0) : values.get(idx);

            Output out = new Output(output.getName(), job, output.getType(), value);
            out.setUsedBy(output.getUsedBy());
            outs.add(out);
        }
        return outs;
    }

    private static List<Input> getCopyOfJobInputs(List<Input> jobInputs, Map<String, List<String>> inputs,
                                                    int idx, Job job, Pipeline pipeline, String baseJob, String fileSeparator) {
        List<Input> copiedInputs = new LinkedList<>();
        for (Input in : jobInputs) {
            List<Input> subInputs = in.getSubInputs();
            String originStep = in.getOriginStep().get(0);
            Job originJob = originStep != null ? originStep.equals(baseJob) ? job : pipeline.getJobById(originStep) : job;
            List<Input> copyOfJobInputs = subInputs == null || subInputs.isEmpty() ?
                                        new LinkedList<>() :
                                        getCopyOfJobInputs(subInputs, inputs, idx, job, pipeline, baseJob, fileSeparator);
            String name = in.getName();
            String value = getValue(inputs, idx, in, name);
//            if (isResourceType(in.getType()) && originStep != null && !originStep.equalsIgnoreCase(baseJob))
            if (job.getSpread() != null)
                value = job.getId() + fileSeparator + value;
            LinkedList<Job> originJobs = new LinkedList<>(Collections.singletonList(originJob));
            copiedInputs.add(new Input(name, originJobs, in.getChainOutput(), in.getType(), value, in.getPrefix(),
                    in.getSeparator(), in.getSuffix(), copyOfJobInputs));
        }
        return copiedInputs;
    }

    private static String getValue(Map<String, List<String>> inputs, int idx, Input input, String name) {
        String value;
        if (inputs.containsKey(name))
            value = inputs.get(name).get(idx);
        else
            value = input.getValue();
        return value;
    }

    private static Environment copyEnvironment(Job job, String jobId, String workingDirectory, String fileSeparator) {
        Environment environment = new Environment();
        String id = fileSeparator + jobId;

        Environment baseEnvironment = job.getEnvironment();
        if (baseEnvironment == null)
            baseEnvironment = JobFactory.getJobEnvironment(job.getId(), workingDirectory, fileSeparator);
        environment.setDisk(baseEnvironment.getDisk());
        environment.setMemory(baseEnvironment.getMemory());
        environment.setCpu(baseEnvironment.getCpu());
        environment.setWorkDirectory(baseEnvironment.getWorkDirectory() + id);
        environment.setOutputsDirectory(baseEnvironment.getOutputsDirectory() + id);

        return environment;
    }

    private static int getInputValuesLength(Map<String, List<String>> valuesOfInputsToSpread) {
        if (valuesOfInputsToSpread.values().iterator().hasNext())
            return valuesOfInputsToSpread.values().iterator().next().size();
        return 0;
    }

    private static Map<String, List<String>> getInputValuesToSpread(Job job, Pipeline pipeline,
                                                                    BiFunction<InOutput, Pipeline, List<String>> getValues,
                                                                    BiFunction<String, Job, List<String>> getOutValues) {
        Map<String, List<String>> valuesOfInputsToSpread = new HashMap<>();
        List<String> inputsToSpread = (List<String>) job.getSpread().getInputs();

        InOutput inOut = new InOutput(null, getOutValues);

        for (Input input : job.getInputs()) {
            inOut.input = input;
            if (inputsToSpread.contains(input.getName())) {
                valuesOfInputsToSpread.put(input.getName(), getValues.apply(inOut, pipeline));
            }
        }

        return valuesOfInputsToSpread;
    }

    private static boolean isSpreadChain(Job job) {
        if (job.getChainsFrom().isEmpty())
            return false;

        for (Input in : job.getInputs()) {
            String originStep = in.getOriginStep().get(0);
            if (originStep != null && !originStep.equals(job.getId())) {
                    return true;
            }
        }
        return false;
    }

    private static SimpleJob getSpreadJob(Job job, Map<String, List<String>> inputs, int idx,
                                          Pipeline pipeline, String fileSeparator) {
        if (job instanceof SimpleJob) {
            SimpleJob simpleJob = (SimpleJob) job;
            ExecutionContext executionContext = simpleJob.getExecutionContext();

            String id = job.getId() + "_" + job.getId() + idx;
            Environment env = copyEnvironment(job, id, pipeline.getEnvironment().getWorkDirectory(), fileSeparator);
            SimpleJob sJob = new SimpleJob(id, env, simpleJob.getCommand(), executionContext);
            sJob.setState(new ExecutionState());
            sJob.getState().setState(job.getState().getState());
            List<Input> jobInputs = getCopyOfJobInputs(job.getInputs(), inputs, idx, sJob, pipeline, job.getId(), fileSeparator);
            sJob.setInputs(jobInputs);
            List<Output> jobOutputs = getCopyOfJobOutputs(job.getOutputs(), sJob, idx);
            sJob.setOutputs(jobOutputs);
            simpleJob.getChainsFrom().forEach(sJob::addChainsFrom);
            simpleJob.getChainsTo().forEach(sJob::addChainsTo);

            return sJob;
        }
        //falta compose jobs
        throw new NotImplementedException();
    }

}
