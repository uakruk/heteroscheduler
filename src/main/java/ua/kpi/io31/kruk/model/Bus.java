package ua.kpi.io31.kruk.model;

import java.util.*;

import static java.lang.Math.*;

/**
 * @author Yaroslav Kruk on 1/12/17.
 *         e-mail: yakruck@gmail.com
 *         GitHub: https://github.com/uakruk
 * @version 1.0
 * @since 1.8
 */
public class Bus {

    static ClockEngine clock = new ClockEngine();

    static final int MARKER_COST = 1;

    int busTime = 0;

    public Queue<Map.Entry<Task, Task>> busQueue =
            new PriorityQueue<>(24, Comparator.comparingInt((Map.Entry<Task, Task> transmission) -> transmission.getKey().getEnd()));

    public Queue<Map.Entry<Task, Task>> taskQueue = new LinkedList<>();

    public int getFreeTime() {

        if (taskQueue.isEmpty()) {
            return 0;
        }

//        Map.Entry<Task, Task> transmission = taskQueue.stream().max(Comparator.comparingInt(entry -> entry.getKey().getEnd() + entry.getKey().getChildren().get(entry.getValue() ))).get();

        Map.Entry<Task, Task> transmission = ((LinkedList<Map.Entry<Task, Task>>)taskQueue).getLast();
        Task sender = transmission.getKey();
        Task receiver = transmission.getValue();

        // get task finish time + time for transmission + marker send delay if processor doesn't have that
        return getDelay();
       // return max(busTime, busTime + sender.getChildren().get(receiver) + (sender.getProcessor().hasMarker() ? 0 : MARKER_COST));
    }

    public int getDelay() {
        int sum = 0;

        Processor prev = null;
        for (Iterator<Map.Entry<Task, Task>> iter = taskQueue.iterator(); iter.hasNext(); ) {
            Map.Entry<Task, Task> transmission = iter.next();
            Task sender = transmission.getKey();
            Task receiver = transmission.getValue();

            if (prev != null && sender.getProcessor() != prev) {
                sum += MARKER_COST;
            }

            if (sender.getEnd() > sum) {
                sum = sender.getEnd();
            }
            sum += sender.getChildren().get(receiver);


            prev = sender.getProcessor();
           // sum += sender.getProcessor().hasMarker() ? 0 : MARKER_COST; // todo chedk
        }
       // System.err.println(sum);
        return sum;
    }

    public void scheduleBus(Task sender, Task receiver) {

        Map.Entry<Task, Task> transmission = new Map.Entry<Task, Task>() {

            private final int delay = getDelay();

            @Override
            public Task getKey() {
                return sender;
            }

            @Override
            public Task getValue() {
                return receiver;
            }

            @Override
            public Task setValue(Task value) {
                return value;
            }

            @Override
            public String toString() {
                int currentPosition = ((LinkedList<Map.Entry<Task, Task>>)taskQueue).indexOf(this);
                int start = 0;
                int end = 0;

                if (currentPosition >= 1) {
                    Map.Entry<Task, Task> prev = ((LinkedList<Map.Entry<Task, Task>>) taskQueue).get(currentPosition - 1);
                }
                start = max(delay, sender.getEnd());
                start += sender.getProcessor().hasMarker() ? 0 : MARKER_COST;
                end = start + sender.getChildren().get(receiver);

                return sender.getId() + " -> " + receiver.getId() + "\t from " +
                        (start + 1) + " to " + (end) + "\n";
            }
        };

        busTime = max(busTime, sender.getEnd()) + sender.getChildren().get(receiver);
        if (!taskQueue.isEmpty()) {
            Map.Entry<Task, Task> lastTransmission = ((LinkedList<Map.Entry<Task, Task>>) taskQueue).getLast();
            Processor lastProcessor = lastTransmission.getKey().getProcessor();

            lastProcessor.sendMarker(sender.getProcessor());
        } else {
            busTime = transmission.getKey().getEnd() + transmission.getKey().getChildren().get(transmission.getValue());
        }

        taskQueue.offer(transmission);
        busQueue.offer(transmission);


    }

    public void removeTransmission(Task sender, Task receiver) {
        busQueue.removeIf(taskTaskEntry -> taskTaskEntry.getKey().equals(sender) && taskTaskEntry.getValue().equals(receiver));
    }

    @Override
    public String toString() {
        return "Bus{" +
                "taskQueue=" + taskQueue +
                '}';
    }
}

