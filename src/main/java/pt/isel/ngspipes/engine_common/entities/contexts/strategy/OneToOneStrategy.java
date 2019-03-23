package pt.isel.ngspipes.engine_common.entities.contexts.strategy;

public class OneToOneStrategy extends CombineStrategy implements IStrategy {

    public OneToOneStrategy(IStrategy firstStrategy, IStrategy secondStrategy) {
        super(firstStrategy, secondStrategy);
    }

    public OneToOneStrategy() { }

}
