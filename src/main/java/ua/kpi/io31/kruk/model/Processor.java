package ua.kpi.io31.kruk.model;

import java.util.Queue;

/**
 * @author Yaroslav Kruk on 1/12/17.
 *         e-mail: yakruck@gmail.com
 *         GitHub: https://github.com/uakruk
 * @version 1.0
 * @since 1.8
 */
public class Processor {

    static TaskScheduler scheduler = TaskScheduler.getInstance();

    private static long counter = 0;

    private final long id = counter++;

    private String name;

    private Marker marker;

    private int speed;

    private Task currentTask;

    private Queue<Task> taskQueue;

    public Processor(int speed) {
        this.speed = speed;
        name = "P" + id + "-" + speed;
    }

    public void sendMarker(Processor processor) {
        processor.setMarker(marker);
        this.marker = null;
    }

    public void executeTask(Task task) {
        this.currentTask = task;
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
}
