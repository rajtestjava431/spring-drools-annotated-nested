package com.example.drools.controller;

import com.example.drools.model.Payload;
import com.example.drools.service.RuleEngineService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RestController
@RequestMapping("/api")
public class RuleController {

    @Autowired
    private RuleEngineService ruleEngineService;

    @PostMapping("/evaluate")
    public Map<String, List<String>> evaluate(@RequestBody Payload payload) throws IllegalAccessException, InterruptedException, IOException {

        AtomicInteger counter = new AtomicInteger(0);
        long startTimeMillis = System.currentTimeMillis();
        // Optional custom thread pool (can also use common ForkJoinPool)
        ExecutorService executor = Executors.newCachedThreadPool();

        // Create 100 tasks using IntStream and CompletableFuture
        List<CompletableFuture<Void>> futures = IntStream.rangeClosed(1, 100)
                .mapToObj(i -> CompletableFuture.runAsync(() -> {
                    String threadName = Thread.currentThread().getName();
                    int taskId = counter.incrementAndGet();
                    System.out.println("Task " + taskId + " is running on " + threadName);

                    // Simulate work
                    try {
                        ruleEngineService.process(payload);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }, executor))
                .collect(Collectors.toList());

        // Wait for all tasks to finish
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        System.out.println("All tasks completed.");

        executor.shutdown();
        System.out.println("Time taken to execute from Controller:" + (System.currentTimeMillis() - startTimeMillis));
        return ruleEngineService.process(payload);
    }
}