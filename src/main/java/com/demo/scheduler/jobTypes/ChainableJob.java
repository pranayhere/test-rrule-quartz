package com.demo.scheduler.jobTypes;

import com.we.recurr.iter.RecurrenceIterator;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;

public abstract class ChainableJob implements Job {
    private static final String CHAIN_JOB_CLASS = "chainedJobClass";
    private static final String CHAIN_JOB_NAME = "chainedJobName";
    private static final String CHAIN_JOB_GROUP = "chainedJobGroup";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'Hmmss'Z'");

    public void execute(JobExecutionContext context) throws JobExecutionException {
        doExecute(context);

        // if chainJob() was called, chain the target job, passing on the JobDataMap
        if (context.getJobDetail().getJobDataMap().get(CHAIN_JOB_CLASS) != null) {
            try {
                chain(context);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        }
    }

    protected void chain(JobExecutionContext context) throws SchedulerException {
        JobDataMap map = context.getJobDetail().getJobDataMap();
        @SuppressWarnings("unchecked")
        Class jobClass = (Class) map.remove(CHAIN_JOB_CLASS);
        String jobName = (String) map.remove(CHAIN_JOB_NAME);
        String jobGroup = (String) map.remove(CHAIN_JOB_GROUP);

        String rrule = map.get("RRULE").toString();

        Iterator<LocalDateTime> itr = new RecurrenceIterator(rrule, null);
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime next = null;
        while (itr.hasNext()) {
            next = itr.next();
            if (next.isAfter(now)) {
                break;
            } else {
                next = null;
            }
        }

        if (next == null) {
            System.out.println("No More jobs to run");
            return;
        }

        rrule = updateRRuleDtStart(rrule, next);

        System.out.println("RRULE PRINTING : " + map.get("RRULE") + "next scheduled at : " + next);
        JobDetail jobDetail = JobBuilder.newJob(jobClass)
                .withIdentity(jobName, jobGroup)
                .usingJobData(map)
                .usingJobData("RRULE", rrule)
                .build();

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(jobName + "Trigger", jobGroup + "Trigger")
                .startAt(Date.from(next.atZone(ZoneId.systemDefault()).toInstant()))
//                .startAt(new Date( System.currentTimeMillis() + 2000L))
                .build();

        System.out.println("Chaining " + jobName);
        StdSchedulerFactory.getDefaultScheduler().scheduleJob(jobDetail, trigger);
    }

    protected String updateRRuleDtStart(String rrule, LocalDateTime next) {
        String formattedDateTime = next.format(formatter);
        StringBuilder newRRule = new StringBuilder();
        for (String component: rrule.split(";")) {
            if (component.startsWith("DTSTART")) {
                String[] parts = component.split("=");
                String directive = parts[0];
                String value = formattedDateTime;

                component = directive + "=" + value;
            }
            newRRule.append(component + ";");
        }
        return newRRule.toString();
    }

    protected abstract void doExecute(JobExecutionContext context) throws JobExecutionException;

    // trigger job chain (invocation waits for job completion)
    protected void chainJob(JobExecutionContext context,
                            Class jobClass,
                            String jobName,
                            String jobGroup) {
        JobDataMap map = context.getJobDetail().getJobDataMap();
        map.put(CHAIN_JOB_CLASS, jobClass);
        map.put(CHAIN_JOB_NAME, jobName);
        map.put(CHAIN_JOB_GROUP, jobGroup);
    }
}
