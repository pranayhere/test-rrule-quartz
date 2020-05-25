package com.demo.scheduler.jobs;

import com.demo.scheduler.jobTypes.ChainableJob;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Random;

public class RecurringRuleJob extends ChainableJob {

    static int COUNT = 0;

    @Override
    protected void doExecute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap map = context.getJobDetail().getJobDataMap();
        System.out.println("Executing " + context.getJobDetail().getKey().getName() + " with " + new LinkedHashMap<>(map));

        map.put("jobTime", new Date().toString());
        map.put("jobValue", new Random().nextLong());

        Random rand = new Random();
        COUNT++;
        chainJob(context, RecurringRuleJob.class, "secondJobName" + COUNT, "secondJobGroup");
    }
}
