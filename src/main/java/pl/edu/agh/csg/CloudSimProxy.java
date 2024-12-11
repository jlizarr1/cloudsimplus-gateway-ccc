package pl.edu.agh.csg;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.cloudlets.CloudletExecution;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.events.SimEvent;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.listeners.CloudletVmEventInfo;
import org.cloudsimplus.listeners.EventListener;
import org.cloudbus.cloudsim.util.DataCloudTags;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelFull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import main.java.pl.edu.agh.csg.VmDescriptor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.Random;
import com.fasterxml.jackson.databind.ObjectMapper;


public class CloudSimProxy {

    public static final String SMALL = "S";
    public static final String MEDIUM = "M";
    public static final String LARGE = "L";

    private static final Logger logger = LoggerFactory.getLogger(CloudSimProxy.class.getName());

    private final DatacenterBrokerFirstFitFixed broker;
    private final CloudSim cloudSim;
    private final SimulationSettings settings;
    private final VmCost vmCost;
    private final Datacenter datacenter;
    private final double simulationSpeedUp;
    private final Map<Long, Double> originalSubmissionDelay = new HashMap<>();
    private final Random random = new Random(System.currentTimeMillis());
    private final List<Cloudlet> jobs = new ArrayList<>();
    private final List<Cloudlet> potentiallyWaitingJobs = new ArrayList<>(1024);
    private final List<Cloudlet> alreadyStarted = new ArrayList<>(128);
    private final Set<Long> finishedIds = new HashSet<>();
    private int toAddJobId = 0;
    private int previousIntervalJobId = 0;
    private int nextVmId;
    private List<? extends Vm> vmList;
    private List<VmDescriptor> vmDescriptors;
    private Map<Integer, Double> interuptionFrequencies = new HashMap<>(); 
    private Map<String, Integer> VmIdStringToInt = new HashMap<>(); 
    private Map<Integer, String> VmIdIntToString = new HashMap<>(); 
    private int numInterruptions = 1;

    public CloudSimProxy(SimulationSettings settings,
                         Map<String, Integer> initialVmsCount,
                         List<CloudletDescriptor> inputJobs,
                         double simulationSpeedUp,
                         List<VmDescriptor> vmDescriptors) {
        this.settings = settings;
        this.cloudSim = new CloudSim(0.01);
        this.broker = createDatacenterBroker();
        this.datacenter = createDatacenter();
        this.vmCost = new VmCost(settings.getVmRunningHourlyCost(),
                simulationSpeedUp,
                settings.isPayingForTheFullHour());
        this.simulationSpeedUp = simulationSpeedUp;
        this.vmDescriptors = vmDescriptors;

        this.nextVmId = 0;

        this.vmList = createVmList();

        broker.submitVmList(vmList);

        this.jobs.addAll(createCloudlets(inputJobs));
        //Collections.sort(this.jobs, new DelayCloudletComparator());
        this.jobs.forEach(c -> originalSubmissionDelay.put(c.getId(), c.getSubmissionDelay()));

        scheduleAdditionalCloudletProcessingEvent(this.jobs);

        this.cloudSim.addOnClockTickListener(eventInfo -> {
            logger.debug("current clock: " + this.cloudSim.clock());
            if (this.cloudSim.clock() == 48.86) {
                Random random = new Random();
                int numVms = this.vmList.size();
                for(int i = 0; i < 3; i++) {
                    int randIndex = random.nextInt(numVms);
                    Vm vmToFail = this.vmList.get(randIndex);
                    destroyVm(vmToFail);
                    logger.debug("VM " + VmIdIntToString.get(Integer.valueOf((int)vmToFail.getId())) + " has gone down at time " + this.cloudSim.clock());
                    numVms--;
                }
                
            }
        });

        this.cloudSim.startSync();
        this.runFor(1000);
    }

    private List<Cloudlet> createCloudlets(List<CloudletDescriptor> jobDescriptors) {
        List<Cloudlet> cloudlets = new ArrayList<>();
        for (CloudletDescriptor descriptor : jobDescriptors) {
            Cloudlet cloudlet = new CloudletSimple(descriptor.getJobId(), descriptor.getMi(), descriptor.getNumberOfCores())
                .setFileSize(DataCloudTags.DEFAULT_MTU)
                .setOutputSize(DataCloudTags.DEFAULT_MTU)
                .setUtilizationModel(new UtilizationModelFull());
            cloudlet.setSubmissionDelay(descriptor.getSubmissionDelay());
            Vm assignedVm = this.vmList.stream()
                .filter(vm -> (int) vm.getId() == this.VmIdStringToInt.get(descriptor.getVmId()))
                .findFirst().get();
            cloudlet.setVm(assignedVm);
            cloudlets.add(cloudlet);
        }
        return cloudlets;
    }

    public boolean allJobsFinished() {
        return this.finishedIds.size() == this.jobs.size();
    }

    public int getFinishedCount() {
        return finishedIds.size();
    }

    private void scheduleAdditionalCloudletProcessingEvent(final List<Cloudlet> jobs) {
        // a second after every cloudlet will be submitted we add an event - this should prevent
        // the simulation from ending while we have some jobs to schedule
        jobs.forEach(c ->
                this.cloudSim.send(
                        datacenter,
                        datacenter,
                        c.getSubmissionDelay() + 1.0,
                        CloudSimTags.VM_UPDATE_CLOUDLET_PROCESSING,
                        ResubmitAnchor.THE_VALUE
                )
        );
    }

    private Datacenter createDatacenter() {
        List<Host> hostList = new ArrayList<>();

        for (int i = 0; i < settings.getDatacenterHostsCnt(); i++) {
            List<Pe> peList = createPeList();

            final long hostRam = settings.getHostRam();
            final long hostBw = settings.getHostBw();
            final long hostSize = settings.getHostSize();
            Host host =
                    new HostWithoutCreatedList(hostRam, hostBw, hostSize, peList)
                            .setRamProvisioner(new ResourceProvisionerSimple())
                            .setBwProvisioner(new ResourceProvisionerSimple())
                            .setVmScheduler(new VmSchedulerTimeShared());

            hostList.add(host);
        }

        return new LoggingDatacenter(cloudSim, hostList, new VmAllocationPolicySimple());
    }

    private List<? extends Vm> createVmList() {
        List<Vm> vmList = new ArrayList<>(this.vmDescriptors.size());

        logger.debug("descriptor size: " + this.vmDescriptors.size());

        for (VmDescriptor descriptor : this.vmDescriptors) {
            vmList.add(createVmWithDescriptor(descriptor));
        }

        return vmList;
    }

    private Vm createVmWithId(String type) {
        int sizeMultiplier = getSizeMultiplier(type);

        Vm vm = new VmSimple(
                this.nextVmId,
                settings.getHostPeMips(),
                settings.getBasicVmPeCnt() * sizeMultiplier);
        this.nextVmId++;
        vm
                .setRam(settings.getBasicVmRam() * sizeMultiplier)
                .setBw(settings.getBasicVmBw())
                .setSize(settings.getBasicVmSize())
                .setCloudletScheduler(new OptimizedCloudletScheduler())
                .setDescription(type);
        vmCost.notifyCreateVM(vm);
        return vm;
    }

    private Vm createVmWithDescriptor(VmDescriptor descriptor) {
        //Continue updating this
        Vm vm = new VmSimple(
                this.nextVmId,
                settings.getHostPeMips(),
                descriptor.getComputePower());
        logger.debug("id: " + descriptor.getVmId());
        logger.debug("compute power: " + descriptor.getComputePower());
        this.VmIdStringToInt.put(descriptor.getVmId(), this.nextVmId);
        this.VmIdIntToString.put(this.nextVmId, descriptor.getVmId());
        logger.debug("Adding VM with ids: " + this.nextVmId + " " + descriptor.getVmId());
        this.interuptionFrequencies.put(this.nextVmId, descriptor.getInteruptionFrequency());
        this.nextVmId++;
        vm
                .setRam(settings.getBasicVmRam())
                .setBw(settings.getBasicVmBw())
                .setSize(settings.getBasicVmSize())
                .setCloudletScheduler(new OptimizedCloudletScheduler())
                .setDescription("" + descriptor.getCost());
        
        vmCost.notifyCreateVM(vm);
        return vm;
    }

    private int getSizeMultiplier(String type) {
        int sizeMultiplier;

        switch (type) {
            case MEDIUM:
                sizeMultiplier = 2; // m5a.xlarge
                break;
            case LARGE:
                sizeMultiplier = 4; // m5a.2xlarge
                break;
            case SMALL:
            default:
                sizeMultiplier = 1; // m5a.large
        }
        return sizeMultiplier;
    }

    private List<Pe> createPeList() {
        List<Pe> peList = new ArrayList<>();
        for (int i = 0; i < settings.getHostPeCnt(); i++) {
            peList.add(new PeSimple(settings.getHostPeMips(), new PeProvisionerSimple()));
        }

        return peList;
    }

    private DatacenterBrokerFirstFitFixed createDatacenterBroker() {
        // this should be first fit
        return new DatacenterBrokerFirstFitFixed(cloudSim);
    }

    public void runFor(final double interval) {
        if(!this.isRunning()) {
            throw new RuntimeException("The simulation is not running - please reset or create a new one!");
        }

        long start = System.nanoTime();
        final double target = this.cloudSim.clock() + interval;

        scheduleJobsUntil(target);

        int i = 0;
        double adjustedInterval = interval;
        while (this.cloudSim.runFor(adjustedInterval) < target) {
            adjustedInterval = target - this.cloudSim.clock();
            adjustedInterval = adjustedInterval <= 0 ? cloudSim.getMinTimeBetweenEvents() : adjustedInterval;

            // Force stop if something runs out of control
            if (i >= 10000) {
                throw new RuntimeException("Breaking a really long loop in runFor!");
            }
            i++;
        }

        alreadyStarted.clear();

        final Iterator<Cloudlet> iterator = potentiallyWaitingJobs.iterator();
        while (iterator.hasNext()) {
            Cloudlet job = iterator.next();
            //remove jobs from the plan that have already succeeded
            if(job.getStatus() == Cloudlet.Status.SUCCESS) {
                try {
                    HttpClient client = HttpClient.newHttpClient();
                    
                    String data = "{\"task_id\": \"" + job.getId() + "\", \"status\": \"completed\"}";
                    
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(new URI("http://127.0.0.1:8000/update_task_status/"))
                            .header("Content-Type", "application/json")
                            .PUT(HttpRequest.BodyPublishers.ofString(data))
                            .build();
                    
                    
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                } catch (IOException ex) {
                } catch (InterruptedException ex) {
                } catch (URISyntaxException ex) {
                }
            }
            if (job.getStatus() == Cloudlet.Status.INEXEC || job.getStatus() == Cloudlet.Status.SUCCESS || job.getStatus() == Cloudlet.Status.CANCELED) {
                alreadyStarted.add(job);
                iterator.remove();
            }
        }

        cancelInvalidEvents();
        printJobStatsAfterEndOfSimulation();

        if(shouldPrintJobStats()) {
            printJobStats();
        }

        // the size of cloudletsCreatedList grows to huge numbers
        // as we re-schedule cloudlets when VMs get killed
        // to avoid OOMing we need to clear that list
        // it is a safe operation in our environment, because that list is only used in
        // CloudSim+ when a VM is being upscaled (we don't do that)
        if(!settings.isStoreCreatedCloudletsDatacenterBroker()) {
            this.broker.getCloudletCreatedList().clear();
        }

        long end = System.nanoTime();
        long diff = end - start;
        double diffInSec = ((double)diff) / 1_000_000_000L;

        // TODO: can be removed after validating the fix of OOM
        // should always be zero
        final int debugBrokerCreatedListSize = this.broker.getCloudletCreatedList().size();
        logger.debug("runFor (" + this.clock() + ") took " + diff + "ns / " + diffInSec + "s (DEBUG: " + debugBrokerCreatedListSize + ")");
    }

    private boolean shouldPrintJobStats() {
        return this.settings.getPrintJobsPeriodically() && Double.valueOf(this.clock()).longValue() % 20000 == 0;
    }

    private void printJobStatsAfterEndOfSimulation() {
        if (!this.isRunning()) {
            logger.info("End of simulation, some reality check stats:");

            printJobStats();
        }
    }

    public void printJobStats() {
        logger.info("All jobs: " + this.jobs.size());
        Map<Cloudlet.Status, Integer> countByStatus = new HashMap<>();
        for (Cloudlet c : this.jobs) {
            final Cloudlet.Status status = c.getStatus();
            int count = countByStatus.getOrDefault(status, 0);
            countByStatus.put(status, count + 1);
        }

        for(Map.Entry<Cloudlet.Status, Integer> e : countByStatus.entrySet()) {
            logger.info(e.getKey().toString() + ": " + e.getValue());
        }

        logger.info("Jobs which are still queued");
        for(Cloudlet cloudlet : this.jobs) {
            if(Cloudlet.Status.QUEUED.equals(cloudlet.getStatus())) {
                printCloudlet(cloudlet);
            }
        }
        logger.info("Jobs which are still executed");
        for(Cloudlet cloudlet : this.jobs) {
            if(Cloudlet.Status.INEXEC.equals(cloudlet.getStatus())) {
                printCloudlet(cloudlet);
            }
        }
    }

    private void printCloudlet(Cloudlet cloudlet) {
        logger.info("Cloudlet: " + cloudlet.getId());
        logger.info("Number of PEs: " + cloudlet.getNumberOfPes());
        logger.info("Number of MIPS: " + cloudlet.getLength());
        logger.info("Submission delay: " + cloudlet.getSubmissionDelay());
        logger.info("Started: " + cloudlet.getExecStartTime());
        final Vm vm = cloudlet.getVm();
        logger.info("VM: " + vm.getId() + "(" + vm.getDescription() + ")"
                + " CPU: " + vm.getNumberOfPes() + "/" + vm.getMips() + " @ " + vm.getCpuPercentUtilization()
                + " RAM: " + vm.getRam().getAllocatedResource()
                + " Start time: " + vm.getStartTime()
                + " Stop time: " + vm.getStopTime());
    }

    private void cancelInvalidEvents() {
        final long clock = (long) cloudSim.clock();

        if (clock % 100 == 0) {
            logger.debug("Cleaning up events (before): " + getNumberOfFutureEvents());
            cloudSim.cancelAll(datacenter, new Predicate<SimEvent>() {

                private SimEvent previous;

                @Override
                public boolean test(SimEvent current) {
                    // remove dupes
                    if (previous != null &&
                            current.getTag() == CloudSimTags.VM_UPDATE_CLOUDLET_PROCESSING &&
                            current.getSource() == datacenter &&
                            current.getDestination() == datacenter &&
                            previous.getTime() == current.getTime() &&
                            current.getData() == ResubmitAnchor.THE_VALUE
                    ) {
                        return true;
                    }

                    previous = current;
                    return false;
                }
            });
            logger.debug("Cleaning up events (after): " + getNumberOfFutureEvents());
        }
    }

    private void scheduleJobsUntil(double target) {
        previousIntervalJobId = nextVmId;
        List<Cloudlet> jobsToSubmit = new ArrayList<>();

        while (toAddJobId < this.jobs.size() && this.jobs.get(toAddJobId).getSubmissionDelay() <= target) {
            // we process every cloudlet only once here...
            final Cloudlet cloudlet = this.jobs.get(toAddJobId);
            // the job shold enter the cluster once target is crossed
            cloudlet.setSubmissionDelay(5.0);
            cloudlet.addOnFinishListener(new EventListener<CloudletVmEventInfo>() {
                @Override
                public void update(CloudletVmEventInfo info) {
                    logger.debug("Cloudlet: " + cloudlet.getId() + "/" + VmIdIntToString.get(Integer.valueOf((int)cloudlet.getVm().getId()))
                            + " Finished.");
                    finishedIds.add(info.getCloudlet().getId());
                }
            });
            jobsToSubmit.add(cloudlet);
            toAddJobId++;

            //simulate possibility of spot instance removal
            //spotInstanceRemoval();
        }

        if (jobsToSubmit.size() > 0) {
            submitCloudletsList(jobsToSubmit);
            potentiallyWaitingJobs.addAll(jobsToSubmit);
        }

        int pctSubmitted = (int) ((toAddJobId / (double) this.jobs.size()) * 10000d);
        int upper = pctSubmitted / 100;
        int lower = pctSubmitted % 100;
        logger.info(
                "Simulation progress: submitted: " + upper + "." + lower + "% "
                        + "Waiting list size: " + this.broker.getCloudletWaitingList().size());
    }

    private void submitCloudletsList(List<Cloudlet> jobsToSubmit) {
        logger.debug("Submitting: " + jobsToSubmit.size() + " jobs");
        broker.submitCloudletList(jobsToSubmit);

        // we immediately clear up that list because it is not really
        // used anywhere but traversing it takes a lot of time
        broker.getCloudletSubmittedList().clear();
    }

    public boolean isRunning() {
        // if we don't have unfinished jobs, it doesn't make sense to execute
        // any actions
        return cloudSim.isRunning() && hasUnfinishedJobs();
    }

    private boolean hasUnfinishedJobs() {
        logger.debug("finished " + this.finishedIds.size() + " total " + this.jobs.size());
        return this.finishedIds.size() < this.jobs.size();
    }

    public long getNumberOfActiveCores() {
        final Optional<Long> reduce = this.broker
                .getVmExecList()
                .parallelStream()
                .map(Vm::getNumberOfPes)
                .reduce(Long::sum);
        return reduce.orElse(0L);
    }

    public double[] getVmCpuUsage() {
        List<Vm> input = broker.getVmExecList();
        double[] cpuPercentUsage = new double[input.size()];
        int i = 0;
        for (Vm vm : input) {
            cpuPercentUsage[i] = vm.getCpuPercentUtilization();
            i++;
        }

        return cpuPercentUsage;
    }

    public int getSubmittedJobsCountLastInterval() {
        return toAddJobId - previousIntervalJobId;
    }

    public int getWaitingJobsCountInterval(double interval) {
        double start = clock() - interval;

        int jobsWaitingSubmittedInTheInterval = 0;
        for (Cloudlet cloudlet : potentiallyWaitingJobs) {
            if (!cloudlet.getStatus().equals(Cloudlet.Status.INEXEC)) {
                double systemEntryTime = this.originalSubmissionDelay.get(cloudlet.getId());
                if (systemEntryTime >= start) {
                    jobsWaitingSubmittedInTheInterval++;
                }
            }
        }
        return jobsWaitingSubmittedInTheInterval;
    }

    public int getSubmittedJobsCount() {
        // this is incremented every time job is submitted
        return this.toAddJobId;
    }

    public double[] getVmMemoryUsage() {
        List<Vm> input = broker.getVmExecList();
        double[] memPercentUsage = new double[input.size()];
        int i = 0;
        for (Vm vm : input) {
            memPercentUsage[i] = vm.getRam().getPercentUtilization();
        }
        return memPercentUsage;
    }

    public void addNewVM(String type) {
        // assuming average delay up to 97s as in 10.1109/CLOUD.2012.103
        // from anecdotal exp the startup time can be as fast as 45s
        Vm newVm = createVmWithId(type);
        double delay = (45 + Math.random() * 52) / this.simulationSpeedUp;
        newVm.setSubmissionDelay(delay);

        broker.submitVm(newVm);
        logger.debug("VM creating requested, delay: " + delay + " type: " + type);
    }

    public boolean removeRandomlyVM(String type) {
        List<Vm> vmExecList = broker.getVmExecList();

        List<Vm> vmsOfType = vmExecList
                .parallelStream()
                .filter(vm -> type.equals((vm.getDescription())))
                .collect(Collectors.toList());

        if (canKillVm(type, vmsOfType.size())) {
            int vmToKillIdx = random.nextInt(vmsOfType.size());
            destroyVm(vmsOfType.get(vmToKillIdx));
            return true;
        } else {
            logger.warn("Can't kill a VM - only one running");
            return false;
        }
    }

    public void spotInstanceRemoval() {
        List<Vm> vmExecList = broker.getVmExecList();
        Random rand = new Random();
        logger.debug("Number of VMs: " + vmExecList.size());
        if(this.numInterruptions > 0 && vmExecList.size() > 1) {
            Vm vm = vmExecList.get(rand.nextInt(vmExecList.size()));
            logger.debug("Chosen VM: " + vm.getId());

            double frequency = interuptionFrequencies.get(Integer.valueOf((int)vm.getId()));
            if(frequency > 0) { //&& interuptionFrequencies.get(vm.getId()) > rand.nextDouble()) {
                updateExecutionPlan(vm.getId());
                destroyVm(vm);
                this.numInterruptions--;
                logger.debug("Killing VM: " + vm.getId());
            }
        }
        
    }

    private Map updateExecutionPlan(long vmId) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String,List<Integer>> plan = new HashMap<>();
        try {
            HttpClient client = HttpClient.newHttpClient();
            
            String data = "{\"instance_id\": \"" + VmIdIntToString.get(Integer.valueOf((int)vmId)) + "\"}";
            
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI("http://127.0.0.1:8000/interrupt_vm/"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(data))
                    .build();
            
            
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            logger.debug("Response Code: " + response.statusCode());
            Map<String,Object> result = objectMapper.readValue(response.body(), Map.class);
            result = (Map<String, Object>) result.get("data");
            plan = (Map<String, List<Integer>>) result.get("execution_plan");
        } catch (IOException ex) {
        } catch (InterruptedException ex) {
        } catch (URISyntaxException ex) {
        }
        return plan;
    }

    private boolean canKillVm(String type, int size) {
        if (SMALL.equals(type)) {
            return size > 1;
        }

        return size > 0;
    }

    private Cloudlet resetCloudlet(Cloudlet cloudlet) {
        cloudlet.setVm(Vm.NULL);
        return cloudlet.reset();
    }

    private List<Cloudlet> resetCloudlets(List<CloudletExecution> cloudlets) {
        return cloudlets
                .stream()
                .map(CloudletExecution::getCloudlet)
                .map(this::resetCloudlet)
                .collect(Collectors.toList());
    }

    private void destroyVm(Vm vm) {
        Map<String,List<Integer>> plan = updateExecutionPlan(vm.getId());
        final String vmSize = vm.getDescription();

        // replaces broker.destroyVm
        List<Cloudlet> allRunningCloudlets = new ArrayList<>();
        allRunningCloudlets.addAll(resetCloudlets(vm.getCloudletScheduler().getCloudletExecList()));
        vm.getHost().destroyVm(vm);
        vm.getCloudletScheduler().clear();
        this.vmList.remove(vm);
        // replaces broker.destroyVm

        for (Vm liveVm : this.vmList) {
            allRunningCloudlets.addAll(resetCloudlets(liveVm.getCloudletScheduler().getCloudletExecList()));
        }

        logger.debug("Killing VM: "
                + vm.getId()
                + " to reschedule cloudlets: "
                + allRunningCloudlets.size()
                + " type: "
                + vmSize);
        if(allRunningCloudlets.size() > 0) {
            rescheduleCloudlets(allRunningCloudlets, plan);
        }
    }

    private void rescheduleCloudlets(List<Cloudlet> affectedCloudlets, Map<String,List<Integer>> plan) {
        final double currentClock = cloudSim.clock();

        long brokerStart = System.nanoTime();
        affectedCloudlets.forEach(cloudlet -> {
            Double submissionDelay = originalSubmissionDelay.get(cloudlet.getId());

            if (submissionDelay == null) {
                throw new RuntimeException("Cloudlet with ID: " + cloudlet.getId() + " not seen previously! Original submission time unknown!");
            }

            if (submissionDelay < currentClock) {
                submissionDelay = 1.0;
            } else {
                // if we the Cloudlet still hasn't been started, let it start at the scheduled time.
                submissionDelay -= currentClock;
            }

            cloudlet.setSubmissionDelay(submissionDelay);

            for (Map.Entry<String, List<Integer>> entry : plan.entrySet()) {
                if(entry.getValue().contains((int) cloudlet.getId())) {
                    Vm assignedVm = this.vmList.stream()
                        .filter(vm -> (int) vm.getId() == this.VmIdStringToInt.get(entry.getKey()))
                        .findFirst().get();
                    cloudlet.setVm(assignedVm);
                    break;
                }
            }
        });
        submitCloudletsList(affectedCloudlets);
        long brokerStop = System.nanoTime();

        double brokerTime = (brokerStop - brokerStart) / 1_000_000_000d;
        logger.debug("Rescheduling " + affectedCloudlets.size() + " cloudlets took (s): " + brokerTime);
    }

    public double clock() {
        return this.cloudSim.clock();
    }

    public long getNumberOfFutureEvents() {
        return this.cloudSim.getNumberOfFutureEvents(simEvent -> true);
    }

    public int getWaitingJobsCount() {
        return this.potentiallyWaitingJobs.size();
    }

    public double getRunningCost() {
        return vmCost.getVMCostPerIteration(this.clock());
    }

    class DelayCloudletComparator implements Comparator<Cloudlet> {

        @Override
        public int compare(Cloudlet left, Cloudlet right) {
            final double diff = left.getSubmissionDelay() - right.getSubmissionDelay();
            if (diff < 0) {
                return -1;
            }

            if (diff > 0) {
                return 1;
            }
            return 0;
        }
    }
}
