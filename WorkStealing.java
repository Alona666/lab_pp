import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class WorkStealing {

    enum TaskDistribution {
        UNIFORM,
        PERIODIC,
        PARETO
    }

    interface Shutdownable {
        void shutdown();
    }

    public interface ShutdownableExecutor extends Shutdownable, Executor {
    }

    static class ThreadPerTaskExecutor implements ShutdownableExecutor {
        List<Thread> threads = new ArrayList<>();

        @Override
        public void execute(Runnable command) {
            var t = new Thread(command);
            threads.add(t);
            t.start();
        }

        public void shutdown() {
            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    static class RoundRobinExecutor implements ShutdownableExecutor {
        protected static final Runnable EXIT_TASK = () -> {
        };

        AtomicInteger counter = new AtomicInteger(0);
        List<Thread> threads;
        Vector<BlockingDeque<Runnable>> tasks;
        volatile boolean isShuttingDown = false;

        RoundRobinExecutor(int threads) {
            Supplier<IntStream> stream = () -> IntStream.iterate(0, x -> x < threads, x -> x + 1);

            tasks = new Vector<>(
                    stream.get()
                            .mapToObj(x -> new LinkedBlockingDeque<Runnable>())
                            .toList()
            );

            this.threads = stream.get()
                    .mapToObj(this::spawnThread)
                    .toList();

            this.threads.forEach(Thread::start);
        }

        Thread spawnThread(int id) {
            return new Thread(() ->
            {
                while (true) {
                    Runnable task;
                    try {
                        task = tasks.get(id).takeFirst();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }

                    if (task == EXIT_TASK)
                        return;

                    task.run();
                }
            }
            );
        }

        @Override
        public void shutdown() {
            synchronized (this) {
                if (!isShuttingDown) {
                    isShuttingDown = true;
                    tasks.forEach(queue -> queue.addLast(EXIT_TASK));
                }
            }

            threads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public synchronized void execute(Runnable command) {
            if (isShuttingDown)
                throw new RejectedExecutionException("Executor is shutting down");

            tasks.get(counter.addAndGet(1) % threads.size()).addLast(command);
        }
    }

    static class WorkStealingExecutor extends RoundRobinExecutor {
        WorkStealingExecutor(int threads) {
            super(threads);
        }

        @Override
        Thread spawnThread(int id) {
            return new Thread(() -> {
                while (true) {
                    Runnable task = tasks.get(id).pollFirst();

                    if (task == null)
                        task = stealTask(id);

                    if (task == null) {
                        try {
                            task = tasks.get(id).pollFirst(1, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }

                        if (task == null)
                            continue;
                    }

                    if (task == EXIT_TASK) {
                        task = stealTask(id);

                        if (task == null)
                            return;

                        tasks.get(id).addLast(EXIT_TASK);
                    }

                    task.run();
                }
            });
        }

        private Runnable stealTask(int thiefId) {
            for (int offset = 1; offset < tasks.size(); offset++) {
                var queue = tasks.get((thiefId + offset) % tasks.size());
                var task = queue.pollLast();

                if (task == EXIT_TASK) {
                    task = queue.pollLast();
                    queue.addLast(EXIT_TASK);
                }

                if (task != null)
                    return task;
            }

            return null;
        }
    }

    /**
     * Обёртка над стандартным FixedThreadPool.
     * Реализует ShutdownableExecutor, чтобы её можно было мерить вместе с остальными.
     */
    static class FixedThreadPoolWrapper implements ShutdownableExecutor {
        private final ExecutorService pool;

        FixedThreadPoolWrapper(int threads) {
            this.pool = Executors.newFixedThreadPool(threads);
        }

        @Override
        public void execute(Runnable command) {
            pool.execute(command);
        }

        @Override
        public void shutdown() {
            if (!pool.isShutdown()) {
                pool.shutdown();
            }

            try {
                // Ждём, пока все задачи реально завершатся.
                while (!pool.awaitTermination(1, TimeUnit.SECONDS)) {
                    // просто ожидание
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                pool.shutdownNow();
            }
        }
    }

    // Это вещи, которые нам нужны
    static final int THREAD_NUMBER = 10;
    static final int TASK_NUMBER = 100_000;
    static final int TARGET_OPTIMAL_FULL_TIME = 10_000; // ms = 10 s

    // Это всякое вспомогательное побочное
    static final int MEAN_TASK_TIME = (int) Math.round((TARGET_OPTIMAL_FULL_TIME + 0d) / TASK_NUMBER * THREAD_NUMBER); // ms
    static final int LOWER_TASK_TIME_BOUND = 0;
    static final int HIGHER_TASK_TIME_BOUND = MEAN_TASK_TIME * 2 + 1;
    static final int ESTIMATED_OPTIMAL_TIME = MEAN_TASK_TIME * TASK_NUMBER / THREAD_NUMBER;
    static final long TARGET_TOTAL_TASK_TIME = (long) TARGET_OPTIMAL_FULL_TIME * THREAD_NUMBER;
    static final long RANDOM_SEED = 42;
    static final double PARETO_SHAPE = 1.5;
    static final int PARETO_RAW_MAX_TASK_TIME = MEAN_TASK_TIME * 100;

    /**
     * По умолчанию false, чтобы случайно не создать 100_000 потоков.
     * Для полного сравнения и с ThreadPerTaskExecutor запускайте:
     * java -DmeasureThreadPerTask=true WorkStealing
     */
    private static final boolean MEASURE_THREAD_PER_TASK = Boolean.getBoolean("measureThreadPerTask");

    /**
     * blackHole: CPU-нагрузка вместо Thread.sleep.
     * Сложность линейна от difficulty.
     * volatile-запись нужна, чтобы JIT не выкинул вычисления как мёртвый код.
     */
    private static volatile long blackHoleSink = 0L;

    /**
     * Сколько итераций blackHole примерно соответствуют 1 миллисекунде.
     * Подбирается калибровкой перед бенчмарком с blackHole.
     */
    private static long blackHoleIterationsPerMillisecond = 100_000L;

    private static final long BLACK_HOLE_WARMUP_ITERATIONS = 1_000_000L;
    private static final long BLACK_HOLE_FIRST_CALIBRATION_ITERATIONS = 10_000_000L;
    private static final long BLACK_HOLE_SECOND_CALIBRATION_ITERATIONS = 100_000_000L;

    static void blackHole(long difficulty) {
        if (difficulty <= 0) {
            return;
        }

        long x = 0x9E3779B97F4A7C15L ^ difficulty;

        for (long i = 0; i < difficulty; i++) {
            x += i;
            x ^= (x << 13);
            x ^= (x >>> 7);
            x ^= (x << 17);
            x *= 0xBF58476D1CE4E5B9L;
        }

        blackHoleSink = x;
    }

    private static long measureBlackHole(long iterations) {
        long start = System.nanoTime();
        blackHole(iterations);
        return System.nanoTime() - start;
    }

    static void calibrateBlackHole() {
        // Прогрев JIT.
        for (int i = 0; i < 3; i++) {
            measureBlackHole(BLACK_HOLE_WARMUP_ITERATIONS);
        }

        long iterations = BLACK_HOLE_FIRST_CALIBRATION_ITERATIONS;
        long elapsedNs = measureBlackHole(iterations);

        long oneMsNs = TimeUnit.MILLISECONDS.toNanos(1);

        // Если первая калибровка слишком быстрая, берём больший объём работы.
        if (elapsedNs < oneMsNs) {
            iterations = BLACK_HOLE_SECOND_CALIBRATION_ITERATIONS;
            elapsedNs = measureBlackHole(iterations);
        }

        if (elapsedNs <= 0) {
            elapsedNs = 1;
        }

        double iterationsPerMs = iterations * 1_000_000d / elapsedNs;
        blackHoleIterationsPerMillisecond = Math.max(1L, Math.round(iterationsPerMs));

        System.out.println("BlackHole calibration: " + blackHoleIterationsPerMillisecond + " iterations/ms");
    }

    static Runnable createTask(int duration, boolean useBlackHole) {
        if (!useBlackHole) {
            return () -> {
                try {
                    Thread.sleep(duration);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
        } else {
            final long difficulty = duration <= 0
                    ? 0L
                    : (long) duration * blackHoleIterationsPerMillisecond;

            return () -> blackHole(difficulty);
        }
    }

    static int createTaskDuration(TaskDistribution distribution, int taskId, Random random) {
        return switch (distribution) {
            case UNIFORM -> random.nextInt(LOWER_TASK_TIME_BOUND, HIGHER_TASK_TIME_BOUND);
            case PERIODIC -> taskId % THREAD_NUMBER == 0
                    ? MEAN_TASK_TIME * THREAD_NUMBER
                    : 0;
            case PARETO -> {
                var scale = MEAN_TASK_TIME * (PARETO_SHAPE - 1) / PARETO_SHAPE;
                var duration = scale / Math.pow(1 - random.nextDouble(), 1 / PARETO_SHAPE);
                yield (int) Math.min(Math.round(duration), PARETO_RAW_MAX_TASK_TIME);
            }
        };
    }

    static int[] createTaskDurations(TaskDistribution distribution) {
        var random = new Random(RANDOM_SEED);
        // var random = java.util.concurrent.ThreadLocalRandom.current();

        var durations = new int[TASK_NUMBER];
        long totalDuration = 0;

        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            durations[taskId] = createTaskDuration(distribution, taskId, random);
            totalDuration += durations[taskId];
        }

        double scale = (double) TARGET_TOTAL_TASK_TIME / totalDuration;
        double remainder = 0;
        long normalizedTotal = 0;

        for (int taskId = 0; taskId < TASK_NUMBER; taskId++) {
            double scaledDuration = durations[taskId] * scale + remainder;
            durations[taskId] = (int) scaledDuration;
            remainder = scaledDuration - durations[taskId];
            normalizedTotal += durations[taskId];
        }

        for (int taskId = 0; normalizedTotal < TARGET_TOTAL_TASK_TIME; taskId++) {
            durations[taskId % TASK_NUMBER]++;
            normalizedTotal++;
        }

        for (int taskId = 0; normalizedTotal > TARGET_TOTAL_TASK_TIME; taskId++) {
            int index = taskId % TASK_NUMBER;
            if (durations[index] > 0) {
                durations[index]--;
                normalizedTotal--;
            }
        }

        return durations;
    }

    static Vector<Runnable> createTasks(TaskDistribution distribution, boolean useBlackHole) {
        var durations = createTaskDurations(distribution);

        return new Vector<>(
                IntStream
                        .iterate(0, x -> x < TASK_NUMBER, x -> x + 1)
                        .mapToObj(x -> createTask(durations[x], useBlackHole))
                        .toList()
        );
    }

    public record Pair<A, B>(A first, B second) {
    }

    public static Pair<Long, Long> measureExecutor(
            ShutdownableExecutor executor,
            TaskDistribution distribution,
            boolean useBlackHole
    ) {
        var tasks = createTasks(distribution, useBlackHole);

        var start = System.nanoTime();
        tasks.forEach(executor::execute);
        var submitionEnd = System.nanoTime();

        executor.shutdown();
        var finish = System.nanoTime();

        return new Pair<>(submitionEnd - start, finish - start);
    }

    public static Pair<Long, Long> measureExecutor(ShutdownableExecutor executor, TaskDistribution distribution) {
        return measureExecutor(executor, distribution, false);
    }

    private static void printResult(String name, Pair<Long, Long> result) {
        System.out.printf("%s submission time: %.3f ms%n", name, result.first / 1_000_000d);
        System.out.printf("%s execution time: %.3f ms%n", name, result.second / 1_000_000d);
    }

    private static void runBenchmark(boolean useBlackHole) {
        String mode = useBlackHole ? "BlackHole" : "Sleep";
        System.out.println("\n===== " + mode + " tasks =====");

        if (MEASURE_THREAD_PER_TASK) {
            var threadPerTaskExecutorResult = measureExecutor(
                    new ThreadPerTaskExecutor(),
                    TaskDistribution.UNIFORM,
                    useBlackHole
            );

            printResult("ThreadPerTaskExecutor (UNIFORM)", threadPerTaskExecutorResult);
        } else {
            System.out.println("ThreadPerTaskExecutor skipped. Use -DmeasureThreadPerTask=true to enable.");
        }

        for (var distribution : TaskDistribution.values()) {
            System.out.println("Task distribution: " + distribution);

            var roundRobinExecutorResult = measureExecutor(
                    new RoundRobinExecutor(THREAD_NUMBER),
                    distribution,
                    useBlackHole
            );
            printResult("RoundRobinExecutor", roundRobinExecutorResult);

            var workStealingExecutorResult = measureExecutor(
                    new WorkStealingExecutor(THREAD_NUMBER),
                    distribution,
                    useBlackHole
            );
            printResult("WorkStealingExecutor", workStealingExecutorResult);

            var fixedThreadPoolResult = measureExecutor(
                    new FixedThreadPoolWrapper(THREAD_NUMBER),
                    distribution,
                    useBlackHole
            );
            printResult("FixedThreadPoolWrapper", fixedThreadPoolResult);
        }
    }

    /*
        Результаты и мысли.

        ВАЖНО:
        Конкретные числа зависят от процессора, ОС, JVM, JIT, температуры CPU и фонового шума.
        Запустите класс и вставьте фактический вывод сюда.

        Формат:
        submission time / execution time, мс.

        =========================================================
        Sleep tasks
        =========================================================

        ThreadPerTaskExecutor UNIFORM:       ___ / ___
        RoundRobinExecutor UNIFORM:          ___ / ___
        WorkStealingExecutor UNIFORM:        ___ / ___
        FixedThreadPoolWrapper UNIFORM:      ___ / ___

        RoundRobinExecutor PERIODIC:         ___ / ___
        WorkStealingExecutor PERIODIC:       ___ / ___
        FixedThreadPoolWrapper PERIODIC:     ___ / ___

        RoundRobinExecutor PARETO:           ___ / ___
        WorkStealingExecutor PARETO:         ___ / ___
        FixedThreadPoolWrapper PARETO:       ___ / ___

        =========================================================
        BlackHole tasks
        =========================================================

        ThreadPerTaskExecutor UNIFORM:       ___ / ___
        RoundRobinExecutor UNIFORM:          ___ / ___
        WorkStealingExecutor UNIFORM:        ___ / ___
        FixedThreadPoolWrapper UNIFORM:      ___ / ___

        RoundRobinExecutor PERIODIC:         ___ / ___
        WorkStealingExecutor PERIODIC:       ___ / ___
        FixedThreadPoolWrapper PERIODIC:     ___ / ___

        RoundRobinExecutor PARETO:           ___ / ___
        WorkStealingExecutor PARETO:         ___ / ___
        FixedThreadPoolWrapper PARETO:       ___ / ___

        =========================================================
        Типичная ожидаемая картина
        =========================================================

        1. ThreadPerTaskExecutor обычно показывает очень большой submission time,
           потому что создаёт по потоку на каждую задачу. Для 100_000 задач это
           стресс для ОС и JVM.

        2. RoundRobinExecutor, WorkStealingExecutor и FixedThreadPoolWrapper
           обычно показывают общее время около TARGET_OPTIMAL_FULL_TIME,
           то есть порядка 10_000 мс, плюс накладные расходы.

        3. FixedThreadPoolWrapper часто является сильным baseline, потому что
           использует одну общую очередь. Для плоского набора задач это означает,
           что длинные задачи не "застревают" в очереди одного потока.

        4. WorkStealingExecutor может выглядеть лучше на PARETO, где есть редкие
           очень длинные задачи. Он пытается украсть задачу с хвоста чужой очереди,
           когда своя очередь пуста.

        5. Однако в этом бенчмарке задачи не рекурсивные и не порождают другие задачи.
           Классический work stealing наиболее полезен именно в таких сценариях,
           например в ForkJoinPool. Поэтому выигрыш WorkStealingExecutor здесь
           может быть небольшим или даже отсутствовать.

        6. Замена sleep на blackHole убирает зависимость от таймера ОС и гранулярности
           Thread.sleep, но добавляет зависимость от скорости CPU, JIT и частоты ядер.
           Калибровка blackHole выполняется однопоточно, поэтому под 10-поточной
           нагрузкой реальная скорость может быть ниже из-за троттлинга, гиперпотоков
           и конкуренции за кэш.

        7. Для PERIODIC большинство задач имеют нулевую длительность. Поэтому результат
           сильно зависит от накладных расходов на вызов Runnable, постановку в очередь
           и пробуждение потоков.

        8. Для PARETO важнее всего балансировка длинного хвоста. Если один поток получил
           несколько длинных задач, остальные могут простаивать. WorkStealing и общий
           очередь FixedThreadPool должны справляться с этим лучше, чем наивный round robin
           с фиксированными очередями, хотя конкретный результат зависит от порядка подачи
           задач и момента старта потоков.
    */

    public static void main(String[] args) {
        System.out.println("Target optimal time: " + TARGET_OPTIMAL_FULL_TIME + " ms");
        System.out.println("Estimated optimal time: " + ESTIMATED_OPTIMAL_TIME + " ms");

        // Первый прогон: задачи спят, как в исходном коде.
        runBenchmark(false);

        // Калибруем blackHole, чтобы 1 ms задачи примерно соответствовал 1 ms CPU-работы.
        calibrateBlackHole();

        // Второй прогон: вместо sleep используется CPU-нагрузка.
        runBenchmark(true);
    }
}
