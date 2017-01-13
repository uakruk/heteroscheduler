package ua.kpi.io31.kruk.model;

import java.util.Map;
import java.util.Set;

/**
 * @author Yaroslav Kruk on 1/12/17.
 *         e-mail: yakruck@gmail.com
 *         GitHub: https://github.com/uakruk
 * @version 1.0
 * @since 1.8
 */
public class Task {

    private static int counter = 0;

    private final int id = counter++;

    private String name;

    private int weight;

    private Set<Task> parents;

    private boolean ready = false;

    private boolean done = false;

    private int start;

    private int end;

    /**
     * a map of dependent tasks and weight of data to send them to
     */
    private Map<Task, Integer> children;

    public Task(int weight) {
        this.weight = weight;
        name = "task-" + id + "/" + weight;
    }

    int getTaskLayer() {
        int layer = 0;
        Task current = this;

        if(!current.getParents().isEmpty()) {
            layer += current.getParents().stream().max((t1, t2) -> {
                int res1 = t1.getTaskLayer();
                int res2 = t2.getTaskLayer();

                return res1 > res2 ? 1 : res1 == res2 ? 0 : -1;
            }).get().getTaskLayer();
        }

        return layer;
    }

    int getMaxLayer() {
        int layer = getTaskLayer();

        if (!this.getChildren().isEmpty()) {
            layer = this.getChildren().keySet().stream().min((t1, t2) -> {
                int res1 = t1.getTaskLayer();
                int res2 = t2.getTaskLayer();

                return res1 > res2 ? 1 : res1 == res2 ? 0 : -1;
            }).get().getTaskLayer();
        }
        return layer;
    }

    boolean canBeExecutedLater() {
        return getMaxLayer() > getTaskLayer();
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public Set<Task> getParents() {
        return parents;
    }

    public void setParents(Set<Task> parents) {
        this.parents = parents;
    }

    public Map<Task, Integer> getChildren() {
        return children;
    }

    public void setChildren(Map<Task, Integer> children) {
        this.children = children;
    }

    public boolean isReady() {
        if (parents.isEmpty()) {
            return true;
        }
        return parents.stream().allMatch(Task::isReady);
    }

    public void setReady(boolean ready) {
        this.ready = ready;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }
}
