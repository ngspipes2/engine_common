package pt.isel.ngspipes.engine_common.utils;

import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.dsl_core.descriptors.pipeline.PipelinesRepositoryFactory;
import pt.isel.ngspipes.dsl_core.descriptors.pipeline.repository.CachePipelinesRepository;
import pt.isel.ngspipes.dsl_core.descriptors.tool.ToolsRepositoryFactory;
import pt.isel.ngspipes.dsl_core.descriptors.tool.repository.CacheToolsRepository;
import pt.isel.ngspipes.pipeline_descriptor.repository.IPipelineRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.IToolRepositoryDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.value.IParameterValueDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.value.ISimpleValueDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.repository.value.IValueDescriptor;
import pt.isel.ngspipes.pipeline_repository.IPipelinesRepository;
import pt.isel.ngspipes.pipeline_repository.PipelinesRepositoryException;
import pt.isel.ngspipes.tool_repository.interfaces.IToolsRepository;
import utils.ToolsRepositoryException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RepositoryUtils {

    public static IRepositoryDescriptor getRepositoryById(Collection<IRepositoryDescriptor> repositoriesDescriptors, String repositoryId) throws EngineCommonException {

        for (IRepositoryDescriptor repository : repositoriesDescriptors)
            if (repository.getId().equals(repositoryId))
                return repository;

        throw new EngineCommonException("Repository with id: " + repositoryId + " not found.");
    }

    public static IPipelinesRepository getPipelinesRepository(IPipelineRepositoryDescriptor repositoryDescriptor, Map<String, Object> params) throws EngineCommonException {
        Map<String, Object> config = createConfig(repositoryDescriptor.getConfiguration(), params);
        String location = repositoryDescriptor.getLocation();

        try {
            IPipelinesRepository pipelinesRepository = PipelinesRepositoryFactory.create(location, config);
            return new CachePipelinesRepository(pipelinesRepository);
        } catch (PipelinesRepositoryException e) {
            throw new EngineCommonException("Error loading pipelines repository: " + location, e);
        }
    }

    public static IToolsRepository getToolsRepository(IToolRepositoryDescriptor repositoryDescriptor, Map<String, Object> params) throws EngineCommonException {
        Map<String, Object> config = createConfig(repositoryDescriptor.getConfiguration(), params);
        String location = repositoryDescriptor.getLocation();

        try {
            IToolsRepository toolsRepository = ToolsRepositoryFactory.create(location, config);
            return new CacheToolsRepository(toolsRepository);
        } catch (ToolsRepositoryException e) {
            throw new EngineCommonException("Error loading tools repository: " + location, e);
        }
    }

    private static Map<String, Object> createConfig(Map<String, IValueDescriptor> configuration, Map<String, Object> params) throws EngineCommonException {
        Map<String, Object> config = new HashMap<>();

        for (Map.Entry<String, IValueDescriptor> entry : configuration.entrySet())
            config.put(entry.getKey(), getValueFromValueDescriptor(entry.getValue(), entry.getKey(), params));

        return config;
    }

    private static Object getValueFromValueDescriptor(IValueDescriptor value, String configurationName, Map<String, Object> params) throws EngineCommonException {
        if(value instanceof IParameterValueDescriptor && params != null && !params.isEmpty())
            return params.get(((IParameterValueDescriptor)value).getParameterName());
        if(value instanceof ISimpleValueDescriptor)
            return ((ISimpleValueDescriptor)value).getValue();

        throw new EngineCommonException("No valid impletation for configuration " + configurationName + " was found.");
    }

}
