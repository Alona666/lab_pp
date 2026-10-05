import java.util.concurrent.atomic.AtomicInteger;

public class pp1 {
    public static final int STEPS = 10_000_000;
    public static final int THREADS = 6;
    public static final int STEPS_PER_THREAD = STEPS / THREADS;
    public static final double A = 0.0;
    public static final double B = Math.PI;
    public static final double DX = (B - A) / STEPS;

    // CPU-bound
    public static double heavyFunction(double x) {
        double res = 0;
        for (int i = 1; i <= 100; i++) {
            res += Math.sin(x * i) * Math.cos(x / i);
        }
        return res;
    }

    // Параллельное вычисление
    public static Thread taskThread(int n, double[] results) {
        return new Thread(() -> {
            int startStep = n * STEPS_PER_THREAD;
            int finishStep = startStep + STEPS_PER_THREAD;
            double localSum = 0;

            for (int i = startStep; i < finishStep; i++) {
                double x = A + i * DX;
                localSum += heavyFunction(x);
            }
            results[n] = localSum * DX;
        });
    }

    public static void measureParallel() throws InterruptedException {
        Thread[] threads = new Thread[THREADS];
        double[] results = new double[THREADS];

        long startTime = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, results);
            threads[i].start();
        }

        for (int i = 0; i < THREADS; i++) {
            threads[i].join();
        }

        double totalSum = 0;
        for (int i = 0; i < THREADS; i++) {
            totalSum += results[i];
        }

        long finishTime = System.nanoTime();
        System.out.println("Parallel Result: " + totalSum);
        System.out.println("Parallel Time (ms): " + (double)(finishTime - startTime) / 1_000_000);
    }

    // Последовательное вычисление
    public static void measureSequential() {
        long startTime = System.nanoTime();
        double sum = 0;

        for (int i = 0; i < STEPS; i++) {
            double x = A + i * DX;
            sum += heavyFunction(x);
        }
        double totalSum = sum * DX;

        long finishTime = System.nanoTime();
        System.out.println("Sequential Result: " + totalSum);
        System.out.println("Sequential Time (ms): " + (double)(finishTime - startTime) / 1_000_000);
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("--- Последовательное вычисление ---");
        measureSequential();

        System.out.println("\n--- Параллельное вычисление ---");
        measureParallel();
    }
}