package com.demo.scheduler;

import com.demo.scheduler.jobs.HelloJob;
import com.demo.scheduler.jobs.RecurringRuleJob;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.Date;
import java.util.List;

public class SchedulerApplication {
    public static void main(String[] args) throws SchedulerException, InterruptedException {
        SchedulerFactory schedFact = new StdSchedulerFactory();
        Scheduler sched = schedFact.getScheduler();
        sched.start();

        JobDetail job = JobBuilder.newJob(RecurringRuleJob.class)
                .withIdentity("firstJob", "firstJobGroup")
                .usingJobData("RRULE", "RRULE:FREQ=DAILY;DTSTART=20200525T91520Z;UNTIL=20200529T183000Z")
                .build();

        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity("firstJobTrigger", "firstJobbTriggerGroup")
                .startAt(new Date(System.currentTimeMillis() + 2000L))
                .withSchedule(SimpleScheduleBuilder
                        .simpleSchedule())
                .build();

        sched.scheduleJob(job, trigger);

        while (true) {
            for (String groupName : sched.getJobGroupNames()) {
                for (JobKey jobKey : sched.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {

                    String jobName = jobKey.getName();
                    String jobGroup = jobKey.getGroup();

                    List<Trigger> triggers = (List<Trigger>) sched.getTriggersOfJob(jobKey);
                    Date nextFireTime = triggers.get(0).getNextFireTime();

                    System.out.println("[jobName] : " + jobName + " [groupName] : " + jobGroup + " - " + nextFireTime);
                }
            }
            Thread.sleep(2000);
        }
    }
}
