package ua.kpi.io31.kruk.model;

import java.util.Comparator;
import java.util.Queue;
import java.util.stream.Collectors;

import static java.lang.Math.*;

/**
 * @author Yaroslav Kruk on 1/12/17.
 *         e-mail: yakruck@gmail.com
 *         GitHub: https://github.com/uakruk
 * @version 1.0
 * @since 1.8
 */
public class Processor {

    static TaskScheduler scheduler = TaskScheduler.getInstance();

    static ClockEngine clock = new ClockEngine();

    static Bus bus = new Bus();

    private static long counter = 0;

    private final long id = counter++;

    private String name;

    private Marker marker;

    private int speed;

    private Task currentTask;

    private Queue<Task> taskQueue;

    public Processor(int speed, Bus bus1) {
        this.speed = speed;
        bus = bus1;
        name = "P" + id + "-" + speed;
    }

    public void sendMarker(Processor processor) {
        processor.setMarker(marker);
        this.marker = null;
    }

    public void executeTask(Task task) {
        this.currentTask = task;
        task.setEnd(task.getStart() + task.getWeight() * speed - 1);
        task.setDone(true); // todo CHECK IT
        task.setProcessor(this);

        for (Task parent : task.getParents().stream().sorted(Comparator.comparingInt(Task::getEnd)).collect(Collectors.toList())) {
            if (parent.getProcessor() != this)
                bus.scheduleBus(parent, task);
        }
    }

    public boolean isBusy() {
        return currentTask != null || !currentTask.isDone();
    }

    public boolean needMarker() {
        return currentTask != null && currentTask.isDone();
    }

    public boolean hasMarker() {
        return marker != null;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Marker getMarker() {
        return marker;
    }

    public void setMarker(Marker marker) {
        this.marker = marker;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public Task getCurrentTask() {
        return currentTask;
    }

    public void setCurrentTask(Task currentTask) {
        this.currentTask = currentTask;
    }

    public int getFreeTime() {
        return currentTask == null ? 0 : currentTask.getEnd();
    }

    @Override
    public String toString() {
        return "Processor{" +
                "name='" + name + '\'' +
                '}';
    }
}
