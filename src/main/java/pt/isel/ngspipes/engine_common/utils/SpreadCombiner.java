package pt.isel.ngspipes.engine_common.utils;

import pt.isel.ngspipes.engine_common.entities.contexts.strategy.ICombineStrategy;
import pt.isel.ngspipes.engine_common.entities.contexts.strategy.IStrategy;
import pt.isel.ngspipes.engine_common.entities.contexts.strategy.InputStrategy;
import pt.isel.ngspipes.engine_common.exception.EngineCommonException;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IOneToManyStrategyDescriptor;
import pt.isel.ngspipes.pipeline_descriptor.step.spread.strategyDescriptor.IOneToOneStrategyDescriptor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SpreadCombiner {


    public static void getInputsCombination(ICombineStrategy strategy, Map<String, List<String>> inputsToSpread) throws EngineCommonException {

        IStrategy first = strategy.getFirstStrategy();
        IStrategy second = strategy.getSecondStrategy();

        if (first instanceof InputStrategy) {
            if (second instanceof InputStrategy) {
                combineInputs(inputsToSpread, strategy);
            } else {
                getInputsCombination((ICombineStrategy) second, inputsToSpread);
            }
        } else if (second instanceof InputStrategy) {
            getInputsCombination((ICombineStrategy) first, inputsToSpread);
        } else {
            getInputsCombination((ICombineStrategy) first, inputsToSpread);
            getInputsCombination((ICombineStrategy) second, inputsToSpread);
        }
    }

    private static void combineInputs(Map<String, List<String>> inputsToSpread,
                                      ICombineStrategy strategy) throws EngineCommonException {
        InputStrategy firstStrategy = (InputStrategy) strategy.getFirstStrategy();
        InputStrategy secondStrategy = (InputStrategy) strategy.getSecondStrategy();
        if (strategy instanceof IOneToOneStrategyDescriptor) {
            ValidateUtils.validateInputValues(inputsToSpread, firstStrategy.getInputName(), secondStrategy.getInputName());
        } else if (strategy instanceof IOneToManyStrategyDescriptor) {
            setOneToMany(firstStrategy.getInputName(), secondStrategy.getInputName(), inputsToSpread);
        }
    }

    private static void setOneToMany(String one, String many, Map<String, List<String>> inputsToSpread) {
        Collection<String> oneValues = inputsToSpread.get(one);
        Collection<String> manyValues = inputsToSpread.get(many);
        inputsToSpread.remove(one);
        inputsToSpread.remove(many);

        List<String> oneCombinedValues = new LinkedList<>();
        List<String> manyCombinedValues = new LinkedList<>();

        for (String oneVal : oneValues) {
            for (String manyVal : manyValues) {
                oneCombinedValues.add(oneVal);
                manyCombinedValues.add(manyVal);
            }
        }

        inputsToSpread.put(one, oneCombinedValues);
        inputsToSpread.put(many, manyCombinedValues);
    }
}
