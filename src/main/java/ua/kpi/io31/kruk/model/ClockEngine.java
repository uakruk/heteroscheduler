package ua.kpi.io31.kruk.model;

import java.util.Comparator;
import java.util.Set;

/**
 * @author Yaroslav Kruk on 1/12/17.
 *         e-mail: yakruck@gmail.com
 *         GitHub: https://github.com/uakruk
 * @version 1.0
 * @since 1.8
 */
public class ClockEngine {

    private static int tick = 0;

    public Processor getNextEvent(Set<Processor> processors) {
        return processors.stream()
                .min(Comparator.comparingInt(processor -> processor.getCurrentTask().getEnd())).get();
    }

    public void next(Set<Processor> processors) {

    }

    public static void nextTick(Set<Processor> processors, Bus bus) {
        bus.taskQueue.peek();
    }

    public static int getTick() {
        return tick;
    }
}
