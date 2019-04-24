package pt.isel.ngspipes.engine_common.utils;

import pt.isel.ngspipes.engine_common.commandBuilders.DockerCommandBuilder;
import pt.isel.ngspipes.engine_common.commandBuilders.ICommandBuilder;
import pt.isel.ngspipes.engine_common.commandBuilders.LocalCommandBuilder;

import java.util.HashMap;
import java.util.Map;

public class CommandBuilderSupplier {

    public final static Map<String, ICommandBuilder> BUILDERS = new HashMap<>();

    static {
        BUILDERS.put("Docker", new DockerCommandBuilder("Z"));
        BUILDERS.put("Local", new LocalCommandBuilder());
    }

    public static ICommandBuilder getCommandBuilder(String commandBuilderName) {
        return BUILDERS.get(commandBuilderName);
    }
}
