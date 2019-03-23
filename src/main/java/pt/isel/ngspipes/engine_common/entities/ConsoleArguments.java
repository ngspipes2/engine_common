package pt.isel.ngspipes.engine_common.entities;

import java.util.Map;

public class ConsoleArguments extends Arguments {

    public String pipes;
    public String intermediateRepresentation;
    public Map<String, Object> parameters;

    public ConsoleArguments() {
    }


    public ConsoleArguments(String pipes, String outPath, int cpus, int mem, int disk,
                            boolean parallel, Map<String, Object> parameters) {
        super(outPath, cpus, mem, disk, parallel);
        this.parameters = parameters;
        this.pipes = pipes;
    }

    public ConsoleArguments(String outPath, int cpus, int mem, int disk, boolean parallel,
                            Map<String, Object> parameters, String ir) {
        super(outPath, parallel);
        this.cpus = cpus;
        this.mem = mem;
        this.disk = disk;
        this.parameters = parameters;
        this.intermediateRepresentation = ir;
    }

}