package pt.isel.ngspipes.engine_common.entities.contexts;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Input {

    private String name;
    private String type;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<String> originStep;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String chainOutput;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String value;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String prefix;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String separator;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String suffix;
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private List<Input> subInputs;
    @JsonIgnore
    private List<Job> originJob;

    public Input(String name, List<Job> originJob, String chainOutput, String type, String value, String prefix,
                 String separator, String suffix, List<Input> subInputs) {
        this.name = name;
        this.chainOutput = chainOutput;
        this.value = value;
        this.type = type;
        this.prefix = prefix;
        this.separator = separator;
        this.suffix = suffix;
        this.subInputs = subInputs;
        this.originJob = originJob;
        setOriginSteps(originJob);
    }

    public Input(String name, List<String> originStep, String chainOutput, String type, String value) {
        this.name = name;
        this.originStep = originStep;
        this.chainOutput = chainOutput;
        this.value = value;
        this.type = type;
    }

    public Input() {}

    public List<String> getOriginStep() { return originStep; }
    public List<Job> getOriginJob() { return originJob; }
    public String getName() { return name; }
    public String getType() { return type; }
    public String getValue() { return value; }
    public String getChainOutput() { return chainOutput; }
    public String getPrefix() { return prefix; }
    public String getSeparator() { return separator; }
    public String getSuffix() { return suffix; }
    public List<Input> getSubInputs() { return subInputs; }

    public void setValue(String value) { this.value = value; }
    public void setOriginJob(List<Job> originJob) {
        this.originJob = originJob;
        setOriginSteps(originJob);
    }

    private void setOriginSteps(List<Job> originJob) {
        List<String> originSteps = new LinkedList<>();
        originJob.forEach((origin) -> originSteps.add(origin.getId()));
        this.originStep = originSteps;
    }

    public void updateOriginJob(Job newOriginJob) {
        this.originJob = new LinkedList<>(Collections.singletonList(newOriginJob));
        this.originStep = new LinkedList<>(Collections.singletonList(newOriginJob.getId()));
    }
}
