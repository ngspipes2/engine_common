package pt.isel.ngspipes.engine_common.entities;

public class Arguments {


    public String outPath;
    public String workingDirectory;
    public int cpus;
    public int mem;
    public int disk;
    public boolean parallel;

    public Arguments() {
    }

    public Arguments(String outPath, String workingDirectory, boolean parallel) {
        this.outPath = outPath;
        this.workingDirectory = workingDirectory;
        this.parallel = parallel;
    }

    public Arguments(String outPath, String workingDirectory, int cpus, int mem, int disk, boolean parallel) {
        this(outPath, workingDirectory, parallel);
        this.outPath = outPath;
        this.cpus = cpus;
        this.mem = mem;
        this.disk = disk;
    }

}
