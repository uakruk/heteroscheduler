package ua.kpi.io31.kruk.model;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Yaroslav Kruk on 1/12/17.
 *         e-mail: yakruck@gmail.com
 *         GitHub: https://github.com/uakruk
 * @version 1.0
 * @since 1.8
 */
public class TaskScheduler {

    private static TaskScheduler instance = new TaskScheduler();

    private TaskScheduler() {}

    public static TaskScheduler getInstance() {
        return instance;
    }

    private Map<Integer, Task> tasks;

    private final int[][] connectionMatrix = {
     //      0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
            {0, 3, 2, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
            {0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 0, 0},//1
            {0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0},//2
            {0, 0, 0, 0, 0, 3, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0},//3
            {0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0},//4
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0},//5
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0},//6
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0},//7
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0},//8
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0},//9
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0},//10
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0},//11
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0},//12
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3},//13
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},//14
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} //15
    };

    private int[] taskWeights = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };

    /**
     *
     * @return a map of task id and task
     */
    public Map<Integer, Task> initializeTasks(int[][] connectionMatrix) {

        Map<Integer, Task> tasks = new HashMap<>();

        // initialize tasks
        for (int weight : taskWeights) {
            Task task = new Task(weight);
            tasks.put(task.getId(), task);
        }

        Map<Integer, Task> response = tasks.values().stream().map( task -> {

            // get dependent tasks
            int[] row = connectionMatrix[task.getId()];
            Map<Task, Integer> children = new HashMap<>();
            Set<Task> parents = new HashSet<>();

            for (int i = 0; i < row.length; i++) {
                if (row[i] != 0) {
                    children.put(tasks.get(i), row[i]);
                }
                // get transitions to this task
                if (connectionMatrix[i][task.getId()] != 0) {
                    parents.add(tasks.get(i));
                }
            }

            task.setChildren(children);
            task.setParents(parents);

            return task;
        }).collect(Collectors.toMap(Task::getId, Function.identity()));

        return response;
    }

    public Map<Integer, Set<Task>> divideByLayers(Map<Integer, Task> tasks) {
        Map<Integer, Set<Task>> response = new HashMap<>();

        for (Task task : tasks.values()) {
            int layer = task.getTaskLayer();
            int maxLayer = task.getMaxLayer();
            int counter = layer;

            // set all possible executive layers for the task
            while (counter++ <= maxLayer) {
                if (!response.containsKey(counter)) {
                    Set<Task> tasksOnLayer = new HashSet<>();

                    tasksOnLayer.add(task);
                    response.put(counter, tasksOnLayer);
                } else {
                    response.get(counter).add(task);
                }
            }
        }

        return response;
    }

    Map<Task, Integer> tLevel(Collection<Task> tasks) {

        Map<Task, Integer> response = new HashMap<>();
        List<Task> taskList = new LinkedList<>(tasks);

        taskList.sort(Comparator.comparingInt(Task::getId));


        taskList.stream().forEachOrdered(task -> {
            int max = 0;
            for(Task parent : task.getParents()) {
                int sum = response.get(parent) + parent.getWeight() + parent.getChildren().get(task);
                if (sum > max) {
                    max = sum;
                }
            }
            response.put(task, max);
        });

        return response;
    }

    Map<Task, Integer> bLevel(Collection<Task> tasks) {
        Map<Task, Integer> response = new HashMap<>();
        List<Task> taskList = new LinkedList<>(tasks);

        taskList.sort(Comparator.comparingInt(Task::getId).reversed());

        taskList.stream().forEachOrdered(task -> {
            int max = 0;
            for (Task child: task.getChildren().keySet()) {
                int sum = response.get(child) + task.getChildren().get(child);
                if (sum > max) {
                    max = sum;
                }
            }
            response.put(task, max + task.getWeight());
        });

        return response;
    }

    int bLevelOnProcessor(Task task, Processor processor, Map<Task, Integer> staticBLevels) {
        return (staticBLevels.get(task) + task.getWeight() * (processor.getSpeed() - 1)); // getBlevel on a processor
    }

    int startTimeOnProcessor(Task task, Processor processor, Map<Task, Integer> tLevels) {
        Map<Task, Integer> children = processor.getCurrentTask().getChildren();
        int oldValue = 0;

        if (children.containsKey(task)) {
            return tLevels.get(task) - children.get(task);
           // oldValue = children.replace(task, 0);
        }
        return tLevels.get(task) - oldValue;
    }

    public Map<Processor, Queue<Task>> schedule(Set<Processor> processors, Set<Task> tasks) {
        Map<Integer, Task> graph = initializeTasks(this.connectionMatrix);
        Map<Processor, Queue<Task>> response = new HashMap<>();

        processors.forEach(processor -> response.put(processor, new LinkedList<>()));

        // 1. Calculate the b-level of each node.
        Map<Task, Integer> bLevels = bLevel(graph.values());

        // 2. Initialize the ready node pool.
        Set<Task> readyPool = graph.values().stream().filter(Task::isReady).collect(Collectors.toSet());

        do {
            // Calculate changed t-levels
            Map<Task, Integer> tLevels = tLevel(readyPool);
            Map<Processor, Map<Task, Integer>> earlyTasks = new HashMap<>();

            // Find the most early task for each processor
            processors.forEach(processor -> {
                Map<Task, Integer> earliestStartTimes = new HashMap<>();

                readyPool.stream().map(task -> earliestStartTimes.put(task, startTimeOnProcessor(task, processor, tLevels)));
                earlyTasks.put(processor, earliestStartTimes);
            });

            int earliestStartTime = Integer.MAX_VALUE;
            Task earliestTask = null;

            Map<Processor, Map<Task, Integer>> DLpairs = new HashMap<>();

            // Calculate the DL for each node-processor pair
            processors.forEach(processor -> {
                Map<Task, Integer> currentTasksOnProcessor = earlyTasks.get(processor);
                Map<Task, Integer> DLpair = new HashMap<Task, Integer>();

                // put current processor to the map
                DLpairs.put(processor, DLpair);

                // calculate the dl for each node on this processor
                currentTasksOnProcessor.entrySet().stream().map(entry -> {
                    int earlyTime = entry.getValue();
                    Task task = entry.getKey();

                    // heterogeneous computing of b-levels
                    int bLevelOfTask = bLevelOnProcessor(task, processor, bLevels);

                    // get the DL for the pair
                    DLpair.put(task, bLevelOfTask - earlyTime);

                    return entry;
                });
            });

            // get the maximum dlvalues for each processor
            Map<Processor, Map.Entry<Task, Integer>> maxDlValues = new HashMap<>();

            DLpairs.forEach((processor, dlPairMap) -> {
                Map.Entry<Task, Integer> maxPair = dlPairMap.entrySet()
                        .stream()
                        .max(Comparator.comparingInt(Map.Entry::getValue)).get();

                maxDlValues.put(processor, maxPair);
            });

            // Find the best sequence of processor - task
            Map.Entry<Processor, Map.Entry<Task, Integer>> bestEntry =
                    maxDlValues.entrySet()
                            .stream()
                            .max(Comparator.comparingInt(entry -> entry.getValue().getValue())).get();

            // schedule and execute this entry on the processor
            Processor executor = bestEntry.getKey();
            Task task = bestEntry.getValue().getKey();

            response.get(executor).offer(task);
            readyPool.remove(task);



        } while (!graph.values().stream().allMatch(Task::isDone));

    }

    Map<Task, Integer> alap(Collection<Task> tasks) {
        Map<Task, Integer> response = new HashMap<>();
        List<Task> taskList = new LinkedList<>(tasks);

        taskList.sort(Comparator.comparingInt(Task::getId).reversed());

        taskList.stream().forEachOrdered(task -> {
            int min_ft = bLevel(tasks).get(task);
            for (Task child : task.getChildren().keySet()) {
                int diff = response.get(child) - task.getChildren().get(child);
                if ( diff < min_ft) {
                    min_ft = diff;
                }
            }
            response.put(task, min_ft - task.getWeight());
        });

        return response;
    }

}
