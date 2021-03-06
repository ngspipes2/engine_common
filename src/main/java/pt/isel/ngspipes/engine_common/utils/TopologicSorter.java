package pt.isel.ngspipes.engine_common.utils;

import pt.isel.ngspipes.engine_common.entities.ExecutionNode;
import pt.isel.ngspipes.engine_common.entities.StateEnum;
import pt.isel.ngspipes.engine_common.entities.contexts.Job;
import pt.isel.ngspipes.engine_common.entities.contexts.Pipeline;

import java.util.*;

public class TopologicSorter {

    public static Collection<ExecutionNode> parallelSort(Pipeline pipeline) {
        List<Job> jobs = pipeline.getJobs();
        List<Job> roots = getRoots(jobs);
        Map<String, Collection<Job>> chainsFrom = getChainFrom(jobs);
        return parallelSort(pipeline, roots, chainsFrom);
    }

    public static Collection<ExecutionNode> parallelSort(Pipeline pipeline, List<Job> jobs) {
        Map<String, Collection<Job>> chainsFrom = getChainFrom(jobs);
        List<Job> roots = getRoots(jobs);
        return parallelSort(pipeline, roots, chainsFrom);
    }

    public static Collection<ExecutionNode> sequentialSort(Pipeline pipeline) {
        Collection<Job> jobs = pipeline.getJobs();
        Map<String, Collection<Job>> chainsFrom = getChainFrom(jobs);
        Map<String, Collection<Job>> chainsTo = getChainTo(jobs);
        List<Job> roots = getRoots(jobs);
        List<ExecutionNode> orderedSteps = getSequentialRoots(getRootJobs(roots));

        while(!roots.isEmpty()) {
            Job root = roots.remove(0);

            if(!chainsFrom.containsKey(root.getId())) {
                if (roots.size() == 0) {
                    return orderedSteps;
                } else {
                    continue;
                }
            }
            for (Job job : chainsFrom.get(root.getId())) {
                String id = job.getId();
                if (chainsTo.containsKey(id))
                    chainsTo.get(id).remove(root);

                if (chainsTo.get(id).isEmpty()) {
                    roots.add(0, job);
                    addSequentialChild(orderedSteps, job);
                }
            }
        }

        return orderedSteps;
    }



    private static Collection<ExecutionNode> parallelSort(Pipeline pipeline, List<Job> roots, Map<String, Collection<Job>> chainsFrom) {
        Collection<Job> jobs = pipeline.getJobs();
        Map<String, Collection<Job>> chainsTo = getChainTo(jobs);
        Collection<ExecutionNode> graph = getRootJobs(roots);

        while(!roots.isEmpty()) {
            Job root = roots.remove(0);

            if (!root.isInconclusive()) {
                String rootID = root.getId();
                if (!chainsFrom.containsKey(rootID)) {
                    if (roots.size() == 0) {
                        return graph;
                    } else {
                        continue;
                    }
                }

                for (Job job : chainsFrom.get(rootID)) {
                    if (chainsTo.containsKey(job.getId()))
                        chainsTo.get(job.getId()).remove(root);

                    if (job.isInconclusive())
                        continue;

                    if (chainsTo.get(job.getId()).isEmpty()) {
                        roots.add(job);
                    }

                    if (job.getSpread() != null){
                        root.setInconclusive(true);
                        setInconclusive(chainsFrom, rootID);
                        break;
                    } else {
                        if (root.getSpread() == null)
                            addToGraphIfAbsent(graph, root, job, pipeline);
                    }
                }
            }
        }
        return graph;
    }

    private static void setInconclusive(Map<String, Collection<Job>> chainsFrom, String rootID) {
        if (chainsFrom.containsKey(rootID)) {
            for (Job child : chainsFrom.get(rootID)) {
                child.setInconclusive(!child.isInconclusive());
            }
        }
    }

    private static List<ExecutionNode> getSequentialRoots(List<ExecutionNode> rootJobs) {
        List<ExecutionNode> nodes = new LinkedList<>();
        nodes.add(rootJobs.get(0));

        for (int idx = 1; idx < rootJobs.size(); idx++)
            addSequentialChild(nodes, rootJobs.get(idx).getJob());

        return nodes;
    }

    private static void addSequentialChild(List<ExecutionNode> orderedSteps, Job job) {
        ExecutionNode node = orderedSteps.get(0);
        if (!node.getJob().getId().equals(job.getId())) {
            if (node.getChilds().isEmpty()) {
                node.getChilds().add(new ExecutionNode(job));
                job.addParent(node.getJob());
            } else
                addSequentialChild(node.getChilds(), job);
        }
    }

    private static void addToGraphIfAbsent(Collection<ExecutionNode> graph, Job root, Job child, Pipeline pipeline) {
        for (ExecutionNode executionNode : graph) {
            Job job = executionNode.getJob();
            if (job.equals(root)) {
                if (notContainsNode(child, executionNode.getChilds())) {
                    executionNode.getChilds().add(new ExecutionNode(child));
                    child.addParent(job);
                }
            } else {
                addToGraphIfAbsent(executionNode.getChilds(), root, child, pipeline);
            }
        }
    }

    private static boolean notContainsNode(Job child, Collection<ExecutionNode> graph) {
        return graph.stream()
                .noneMatch((e) -> e.getJob().equals(child));
    }

    private static List<ExecutionNode> getRootJobs(List<Job> roots) {
        List<ExecutionNode> executionNodes = new LinkedList<>();

        for (Job job : roots) {
            ExecutionNode executionNode = new ExecutionNode(job);
            executionNodes.add(executionNode);
        }

        return executionNodes;
    }

    private static List<Job> getRoots(Collection<Job> jobs) {
        List<Job> rootJobs = new LinkedList<>();
        for (Job job : jobs) {
            if (job.getChainsFrom().isEmpty() || (!job.getChainsFrom().isEmpty() && areParentSuccess(job.getChainsFrom())))
                rootJobs.add(job);
        }
        return rootJobs;
    }

    private static boolean areParentSuccess(List<Job> chainsFrom) {
        for (Job job : chainsFrom) {
            if (!isJobSuccess(job) && !job.isInconclusive())
                return false;
        }
        return true;
    }

    private static boolean isJobSuccess(Job job) {
        return job.getState() != null && job.getState().getState() != null && job.getState().getState().equals(StateEnum.SUCCESS);
    }

    private static Map<String, Collection<Job>> getChainTo(Collection<Job> jobs) {
        Map<String, Collection<Job>> chainTo = new HashMap<>();
        for (Job job : jobs) {
            if (!isJobSuccess(job))
                chainTo.put(job.getId(), new LinkedList<>(job.getChainsFrom()));
        }
        return chainTo;
    }

    private static Map<String, Collection<Job>> getChainFrom(Collection<Job> jobs) {
        Map<String, Collection<Job>> chainFrom = new HashMap<>();
        for (Job job : jobs) {
            if (!isJobSuccess(job))
                chainFrom.put(job.getId(), new LinkedList<>(job.getChainsTo()));
        }
        return chainFrom;
    }
}
