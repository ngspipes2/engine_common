package pt.isel.ngspipes.engine_common.exception;

public class InputValidationException extends EngineCommonException {

    public InputValidationException(String msg) {
        super(msg);
    }

    public InputValidationException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
