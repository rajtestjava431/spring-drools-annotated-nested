package com.example.drools.service;

import com.example.drools.model.Group;
import com.example.drools.model.Payload;
import jakarta.annotation.PostConstruct;
import org.kie.api.KieBase;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.utils.KieHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.springframework.core.io.Resource;

@Service
public class RuleEngineService {
    @Autowired
    private KieContainer kieContainer;

    Map<String, KieBase> compiledRulesCache = new ConcurrentHashMap<>();

    public List<String> listFilesInClasspathFolder(String folderName) throws IOException {
        URL url = Thread.currentThread().getContextClassLoader().getResource(folderName);
        if (url == null) {
            throw new IllegalArgumentException("Folder not found: " + folderName);
        }

        File folder = new File(url.getPath());
        if (!folder.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + folderName);
        }

        return Arrays.stream(Objects.requireNonNull(folder.listFiles()))
                .map(File::getName)
                .collect(Collectors.toList());
    }

   /* public static KieSession loadAllDrlFiles(String folderPath) throws IOException {
        KieHelper kieHelper = new KieHelper();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        // Match all .drl files under the given folder
        Resource[] drlResources = resolver.getResources("classpath*:" + folderPath + "/*.drl");

        for (Resource resource : drlResources) {
            System.out.println("Loading DRL: " + resource.getFilename());
            try (InputStream input = resource.getInputStream()) {
                kieHelper.addResource(
                        org.kie.internal.io.ResourceFactory.newInputStreamResource(input),
                        ResourceType.DRL
                );
            }
        }

        return kieHelper.build().newKieSession();
    }*/
    //@PostConstruct
    public KieBase getOrBuildKieBase(String groupName) {
        return compiledRulesCache.computeIfAbsent(groupName, g -> {
            KieHelper helper = new KieHelper();
            helper.addResource(ResourceFactory.newClassPathResource(g + "-rules.drl"), ResourceType.DRL);
            helper.build().getKiePackages().forEach(pkg -> {
                System.out.println("Package name:"+pkg.getName());
                pkg.getRules().forEach(rule -> System.out.println("Rule loaded in " + groupName + ": " + rule.getName()));
            });
            return helper.build();
        });
    }

    public void warmUpAllRules(Set<String> groupNames) {
        System.out.println("Warming up Drools rules...");

        for (String group : groupNames) {
            try {
                System.out.println("Warm-up: Compiling " + group + "-rules.drl");

                KieHelper kieHelper = new KieHelper();
                kieHelper.addResource(
                        ResourceFactory.newClassPathResource(group + "-rules.drl"),
                        ResourceType.DRL
                );

                // Just compile; no need to keep session
                kieHelper.build().newKieSession().dispose();

                System.out.println("Warm-up successful for " + group);
            } catch (Exception e) {
                System.err.println("Warm-up failed for " + group + ": " + e.getMessage());
            }
        }

        System.out.println("All rules warmed up.");
    }

    public Map<String, List<String>> process(Payload payload) throws IllegalAccessException, InterruptedException, IOException {
        long startTimeMillis = System.currentTimeMillis();
        System.out.println("Start time :");
        Map<String, Map<String, Object>> groupedFields = new HashMap<>();
        extractGroupedFields(payload, "", groupedFields);
       /* warmUpAllRules(groupedFields.keySet());

        List<String> files = listFilesInClasspathFolder("rules");
        files.forEach(System.out::println);*/
        groupedFields.keySet().forEach(this::getOrBuildKieBase);

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

                   /* KieHelper kieHelper = new KieHelper();
                    kieHelper.addResource(
                            ResourceFactory.newClassPathResource(groupName + "-rules.drl"),
                            ResourceType.DRL
                    );

                    kieHelper.build().getKiePackages().forEach(pkg -> {
                        System.out.println("Package name:"+pkg.getName());
                        pkg.getRules().forEach(rule -> System.out.println("Rule loaded in " + groupName + ": " + rule.getName()));
                    });
                    KieSession session = kieHelper.build().newKieSession();*/
                    KieSession session = getOrBuildKieBase(groupName).newKieSession();

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
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[groupedFields.size()])).join();

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