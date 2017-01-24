package ua.kpi.io31.kruk.model;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.*;

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

    public Map<Task, Integer> tLevels = new HashMap<>();

    private final int[][] connectionMatrix = {
     //      0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
            {0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},//0
            {0, 0, 0, 0, 0, 3, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0},//1
            {0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0},//2
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0},//3
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0},//4
            {0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 2, 0, 0, 0, 0},//5
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 4, 3, 0, 0, 0, 0},//6
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0},//7
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},//8
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 0, 0},//9
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0},//10
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0},//11
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4},//12
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4},//13
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},//14
            {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0} //15
    };

    private int[] taskWeights = { 7, 5, 2, 4, 9, 6, 7, 8, 3, 4, 1, 2, 5, 6, 3, 9 };

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

        List<Task> taskList = new LinkedList<>(tasks);

        taskList.sort(Comparator.comparingInt(Task::getId));


        taskList.stream().forEachOrdered(task -> {
            int max = 0;
            for(Task parent : task.getParents()) {
                int sum = (parent.isDone() ? parent.getStart() : tLevels.get(parent))
                        + parent.getWeight()
                        + parent.getChildren().get(task) /* + Arrays.stream(connectionMatrix[parent.getId()]).sum()*/; // plus edge weight
                if (sum > max) {
                    max = sum;
                }
            }
            tLevels.put(task, max);
        });

        return tLevels;
    }

    Map<Task, Integer> bLevel(Collection<Task> tasks, Set<Processor> processors) {

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
            response.put(task, max + (int) processors.stream()
                    .mapToInt(p -> p.getSpeed() * task.getWeight())
                    .average().getAsDouble());
        });

        return response;
    }

    int bLevelOnProcessor(Task task, Processor processor, Map<Task, Integer> staticBLevels) {
        return (staticBLevels.get(task) + deltaTaskProcessor(task, processor, staticBLevels.get(task))); // getBlevel on a processor
    }

    int startTimeOnProcessor(Task task, Processor processor, Bus bus, Map<Task, Integer> tLevels) {
        //Map<Task, Integer> children = processor.getCurrentTask().getChildren();
        Set<Task> parents = task.getParents();

        int busStartTime = bus.getFreeTime();
        int summaryTime = 0;


        List<Task> sortedParents = parents.stream().sorted(Comparator.comparingInt(Task::getEnd)).collect(Collectors.toList());
        // get the earliest time of parent's execution finish
        int earliestFinishOfParent =  parents.isEmpty() ? 0 : parents.stream().min(Comparator.comparingInt(Task::getEnd)).get().getEnd();
        //
        summaryTime += max(busStartTime, max(earliestFinishOfParent, processor.getCurrentTask() != null ? processor.getCurrentTask().getEnd() : 0));

        Processor previous = null;
        for (Task parent: sortedParents) {
            if (parent.getProcessor() != processor) {
                // calculate time for transmission

                if (previous == null) {
                    summaryTime += parent.getProcessor().hasMarker() ? 0 : Bus.MARKER_COST;
                } else {
                    summaryTime += (previous == parent.getProcessor()) ? 0 : Bus.MARKER_COST;
                }
                previous = parent.getProcessor();
                if (parent.getEnd() > summaryTime) {
                    summaryTime = parent.getEnd();
                }
                summaryTime += parent.getChildren().get(task);// + (processor.hasMarker() ? 0 : Bus.MARKER_COST);
            }
        }

        return summaryTime;
    }

    int executeTimeOnProcessor(Task task, Processor processor) {
        return task.getWeight() * processor.getSpeed();
    }

    int deltaTaskProcessor(Task task, Processor processor, int staticBLevel) {
        return staticBLevel - executeTimeOnProcessor(task, processor);
    }

    void updateReadyPool(Set<Task> readyPool, Collection<Task> allTasks) {
        readyPool.addAll(allTasks.stream().filter(Task::isReady).collect(Collectors.toSet()));
    }

    public Map<Processor, Queue<Task>> schedule(Set<Processor> processors, Bus bus, Set<Task> tasks) {
        Map<Integer, Task> graph = initializeTasks(this.connectionMatrix);
        Map<Processor, Queue<Task>> response = new HashMap<>();

        processors.forEach(processor -> response.put(processor, new LinkedList<>()));

        // 1. Calculate the b-level of each node.
        Map<Task, Integer> bLevels = bLevel(graph.values(), processors);

        // 2. Initialize the ready node pool.
        Set<Task> readyPool = graph.values().stream().filter(Task::isReady).collect(Collectors.toSet());

        do {
            // Calculate changed t-levels
            Map<Task, Integer> tLevels = tLevel(readyPool);
            Map<Processor, Map<Task, Integer>> earlyTasks = new HashMap<>();

            // Find the most early task for each processor
            processors.forEach(processor -> {

                Map<Task, Integer> earliestStartTimes = new HashMap<>();

                // for each ready for execution task compute the earliest start time
                readyPool.forEach(task -> {
                    earliestStartTimes.put(task, startTimeOnProcessor(task, processor, bus, tLevels));
                });
                earlyTasks.put(processor, earliestStartTimes);
            });


            Map<Processor, Map<Task, Integer>> DLpairs = new HashMap<>();

            // Calculate the DL for each node-processor pair
            processors.forEach(processor -> {
                Map<Task, Integer> currentTasksOnProcessor = earlyTasks.get(processor);
                Map<Task, Integer> DLpair = new HashMap<Task, Integer>();

                // put current processor to the map
                DLpairs.put(processor, DLpair);
                // calculate the dl for each node on this processor
                currentTasksOnProcessor.entrySet().forEach(entry -> {

                    int earlyTime = entry.getValue();
                    Task task = entry.getKey();

                    // heterogeneous computing of b-levels
                    int bLevelOfTask = bLevelOnProcessor(task, processor, bLevels);

                    // get the DL for the pair
                    DLpair.put(task, bLevelOfTask - max(earlyTime, processor.getFreeTime()));

                });
            });

            // get the maximum dlvalues for each processor
            Map<Processor, Map.Entry<Task, Integer>> maxDlValues = new HashMap<>();


            DLpairs.forEach((processor, dlPairMap) -> {
                Map.Entry<Task, Integer> maxPair = dlPairMap.entrySet()
                        .stream()
                        .max(Comparator.comparingInt(Map.Entry::getValue)).get();
                if (maxPair.getKey().getId() > 12) {
                    System.out.println(processor + "\t" + maxPair + "\t" + startTimeOnProcessor(maxPair.getKey(), processor, bus, tLevels));
                }
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
            int time = startTimeOnProcessor(task, executor, bus, tLevels);
            time += task.getParents().isEmpty() ? 0 : 1;

            tLevels.replace(task, time);
            task.setStart(time);

            response.get(executor).offer(task);
            readyPool.remove(task);

            executor.executeTask(task);

            updateReadyPool(readyPool, graph.values());

        } while (!graph.values().stream().allMatch(Task::isDone));

        return response;
    }

    public static void main(String[] args) {
        TaskScheduler scheduler = new TaskScheduler();
        Set<Processor> processors = new HashSet<>();
        Bus bus = new Bus();

        Processor p1 = new Processor(1, bus);
        p1.setMarker(Marker.MARKER);
        processors.add(p1);

        for (int i = 0; i < 3; i++) {
            processors.add(new Processor((i % 2 == 0 ? 2 : 1), bus));
        }

        Map<Processor, Queue<Task>> result = scheduler.schedule(processors, bus, null);

        for (Map.Entry<Processor, Queue<Task>> entry : result.entrySet()) {
            System.out.println(entry.getKey() + ":");
            for (Task task : entry.getValue()) {
                System.out.println(task);
            }
        }
        System.out.println(bus);
    }

}
