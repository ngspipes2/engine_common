package pt.isel.ngspipes.engine_common.executionReporter;

import pt.isel.ngspipes.engine_common.exception.ProgressReporterException;

public class DummyReporter implements IExecutionProgressReporter {
    @Override
    public void open() throws ProgressReporterException { }

    @Override
    public void reportTrace(String msg) throws ProgressReporterException { }

    @Override
    public void reportError(String msg) throws ProgressReporterException { }

    @Override
    public void reportInfo(String msg) throws ProgressReporterException { }

    @Override
    public void close() throws ProgressReporterException { }
}
