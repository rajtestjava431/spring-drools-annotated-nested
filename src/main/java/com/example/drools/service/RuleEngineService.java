package com.example.drools.service;

import com.example.drools.model.Group;
import com.example.drools.model.Payload;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.utils.KieHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
public class RuleEngineService {
    @Autowired
    private KieContainer kieContainer;

    public Map<String, List<String>> process(Payload payload) throws IllegalAccessException, InterruptedException {
        long startTimeMillis = System.currentTimeMillis();
        System.out.println("Start time :");
        Map<String, Map<String, Object>> groupedFields = new HashMap<>();
        extractGroupedFields(payload, "", groupedFields);

        Map<String, List<String>> ruleSummary = new HashMap<>();


        ruleSummary = getResultsummaryUsingPool(groupedFields);


        //evalutateUsingForloop(groupedFields, ruleSummary);
        System.out.println("Time taken to execute :" + (System.currentTimeMillis() - startTimeMillis));
        return ruleSummary;
    }

    private static void evalutateUsingForloop(Map<String, Map<String, Object>> groupedFields, Map<String, List<String>> ruleSummary) {
        for (Map.Entry<String, Map<String, Object>> entry : groupedFields.entrySet()) {
            String groupName = entry.getKey();
            //KieSession session = kieContainer.newKieSession();
            KieHelper kieHelper = new KieHelper();
            kieHelper.addResource(ResourceFactory.newClassPathResource(groupName+"-rules.drl"), ResourceType.DRL);
            KieSession session = kieHelper.build().newKieSession();

            List<String> firedRules = new ArrayList<>();
            session.addEventListener(new org.kie.api.event.rule.DefaultAgendaEventListener() {
                @Override
                public void afterMatchFired(org.kie.api.event.rule.AfterMatchFiredEvent event) {
                    firedRules.add(event.getMatch().getRule().getName());
                }
            });

            session.insert(entry.getValue());
            session.getAgenda().getAgendaGroup(groupName + "Rules").setFocus();
            session.fireAllRules();
            session.dispose();

            ruleSummary.put(groupName, firedRules);
        }
        //return ruleSummary;
    }

    private Map<String, List<String>> getResultsummaryUsingPool(Map<String, Map<String, Object>> groupedFields) throws InterruptedException {

        Map<String, List<String>> ruleSummary = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(groupedFields.size());

        List<CompletableFuture<Void>> futures = groupedFields.entrySet().stream()
                .map(entry -> CompletableFuture.runAsync(() -> {
                    String groupName = entry.getKey();
                    Map<String, Object> facts = entry.getValue();

                    KieHelper kieHelper = new KieHelper();
                    kieHelper.addResource(
                            ResourceFactory.newClassPathResource(groupName + "-rules.drl"),
                            ResourceType.DRL
                    );
                    KieSession session = kieHelper.build().newKieSession();

                    List<String> firedRules = new ArrayList<>();
                    session.addEventListener(new org.kie.api.event.rule.DefaultAgendaEventListener() {
                        @Override
                        public void afterMatchFired(org.kie.api.event.rule.AfterMatchFiredEvent event) {
                            firedRules.add(event.getMatch().getRule().getName());
                        }
                    });

                    session.insert(facts);
                    session.getAgenda().getAgendaGroup(groupName + "Rules").setFocus();
                    session.fireAllRules();
                    session.dispose();

                    ruleSummary.put(groupName, firedRules);
                }, executor)).collect(Collectors.toList());

        // Wait for all tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        executor.shutdown();

        return ruleSummary;
    }


    private void extractGroupedFields(Object obj, String path, Map<String, Map<String, Object>> groupedFields) throws IllegalAccessException {
        if (obj == null) return;

        Class<?> clazz = obj.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            Object value = field.get(obj);
            String currentPath = path.isEmpty() ? field.getName() : path + "." + field.getName();

            if (field.isAnnotationPresent(Group.class)) {
                String group = field.getAnnotation(Group.class).value();
                groupedFields.computeIfAbsent(group, k -> new HashMap<>()).put(currentPath, value);
            }

            if (value != null && !isJavaBuiltin(field.getType())) {
                extractGroupedFields(value, currentPath, groupedFields);
            }
        }
    }

    private boolean isJavaBuiltin(Class<?> clazz) {
        return clazz.isPrimitive() ||
                clazz.getName().startsWith("java.") ||
                clazz.isEnum() ||
                clazz == String.class;
    }
}