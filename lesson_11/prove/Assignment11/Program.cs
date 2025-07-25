﻿using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace assignment11
{
    public class Assignment11
    {
        private const long START_NUMBER = 10_000_000_000;
        private const int RANGE_COUNT   = 1_000_000;

        private static bool IsPrime(long n)
        {
            if (n <= 3) return n > 1;
            if (n % 2 == 0 || n % 3 == 0) return false;

            for (long i = 5; i * i <= n; i += 6)
            {
                if (n % i == 0 || n % (i + 2) == 0)
                    return false;
            }
            return true;
        }

        public static void Main(string[] args)
        {
            // Use local variables for counting since we are in a single thread.
            int numbersProcessed = 0;
            int primeCount       = 0;

            Console.WriteLine("Prime numbers found:");

            var stopwatch = Stopwatch.StartNew();
            
            // A single for-loop to check every number sequentially.
            var queue = new ConcurrentQueue<long>();
            for (long i = START_NUMBER; i < START_NUMBER + RANGE_COUNT; i++)
                queue.Enqueue(i);

            int threadCount = 10;
            if (args.Length > 0 && int.TryParse(args[0], out int t) && t > 0)
                threadCount = t;

            object writeLock = new object();
            var threads = new Thread[threadCount];
            for (int w = 0; w < threadCount; w++)
            {
                threads[w] = new Thread(() =>
                {
                    while (queue.TryDequeue(out long n))
                    {
                        Interlocked.Increment(ref numbersProcessed);
                        if (IsPrime(n))
                        {
                            Interlocked.Increment(ref primeCount);
                            lock (writeLock)
                            {
                                Console.Write($"{n}, ");
                            }
                        }
                    }
                });
                threads[w].Start();
            }

            foreach (var th in threads)
                th.Join();

            stopwatch.Stop();

            Console.WriteLine(); // New line after all primes are printed
            Console.WriteLine();

            // Should find 43427 primes for range_count = 1000000
            Console.WriteLine($"Numbers processed = {numbersProcessed}");
            Console.WriteLine($"Primes found      = {primeCount}");
            Console.WriteLine($"Total time        = {stopwatch.Elapsed}");
        }
    }
}
