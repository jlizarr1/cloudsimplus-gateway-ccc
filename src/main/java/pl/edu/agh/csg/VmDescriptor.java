package main.java.pl.edu.agh.csg;

import java.util.Objects;

public class VmDescriptor {
    private int vmId;
    private int computePower;
    private double interuptionFrequency;
    private double cost;

    public VmDescriptor(int vmId, int computePower, double interuptionFrequency, double cost) {
        this.vmId = vmId;
        this.computePower = computePower;
        this.interuptionFrequency = interuptionFrequency;
        this.cost = cost;
    }

    public int getVmId() {
        return vmId;
    }

    public int getComputePower() {
        return computePower;
    }

    public double getInteruptionFrequency() {
        return interuptionFrequency;
    }

    public double getCost() {
        return cost;
    }

    // public Cloudlet toCloudlet() {
    //     Cloudlet cloudlet = new CloudletSimple(jobId, mi, numberOfCores)
    //             .setFileSize(DataCloudTags.DEFAULT_MTU)
    //             .setOutputSize(DataCloudTags.DEFAULT_MTU)
    //             .setUtilizationModel(new UtilizationModelFull());
    //     cloudlet.setSubmissionDelay(submissionDelay);
    //     //TODO: need to get the vms in the system
    //     //cloudlet.setVm(assignedVmId);
    //     return cloudlet;
    // }
}
