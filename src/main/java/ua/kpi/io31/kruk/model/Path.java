package ua.kpi.io31.kruk.model;

/**
 * @author Yaroslav Kruk on 1/13/17.
 *         e-mail: yakruck@gmail.com
 *         GitHub: https://github.com/uakruk
 * @version 1.0
 * @since 1.8
 */
public class Path {

    private Task task;

    private Path prev;

    public Path(Task task) {
        this.task = task;
        this.prev = null;
    }

    public Path(Task task, Path path) {
        this.task = task;
        this.prev = path;
    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public Path getPrev() {
        return prev;
    }

    public void setPrev(Path prev) {
        this.prev = prev;
    }
}
