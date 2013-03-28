package com.weibo.api.platform.prism.storm.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class PrismScheduler implements IScheduler {
	private static final Logger LOG = Logger.getLogger(PrismScheduler.class);

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		String topologyId = "PRISM-TOPOLOGY";
		
		TopologyDetails topology = topologies.getByName(topologyId);
		if (topology != null) {
            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
            	LOG.info(topologyId + ": Our special topology DOES NOT NEED scheduling.");
            } else {
            	LOG.info(topologyId + ": Our special topology needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                
                LOG.info(topologyId + ": needs scheduling(component->executor): " + componentToExecutors);
                LOG.info(topologyId + ": needs scheduling(executor->compoenents): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topologies.getByName(topologyId).getId());
                LOG.info(topologyId + ":current assignments: " + (currentAssignment == null ? "{}" : currentAssignment.getExecutorToSlot()));
                
                if (!componentToExecutors.containsKey("spout.id.prism.scribe")) {
                	LOG.info(topologyId + " Our special-spout DOES NOT NEED scheduling.");
                } else {
                	LOG.info(topologyId + " Our special-spout needs scheduling.");
                    List<ExecutorDetails> executors = componentToExecutors.get("spout.id.prism.scribe");

                    // find out the our "special-supervisor" from the supervisor metadata
                    Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
                    SupervisorDetails specialSupervisor = null;
                    for (SupervisorDetails supervisor : supervisors) {
                        Map meta = (Map) supervisor.getSchedulerMeta();
                        LOG.info("meta:" + meta);
                        if (meta != null && meta.get("name") != null && meta.get("name").equals("prism-spout-supervisor")) {
                            specialSupervisor = supervisor;
                            break;
                        }
                    }

                    // found the special supervisor
                    if (specialSupervisor != null) {
                    	LOG.info("Found the special-supervisor");
                        List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
                        
                        // if there is no available slots on this supervisor, free some.
                        // TODO for simplicity, we free all the used slots on the supervisor.
                        if (availableSlots.isEmpty() && !executors.isEmpty()) {
                            for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
                                cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
                            }
                        }

                        // re-get the aviableSlots
                        availableSlots = cluster.getAvailableSlots(specialSupervisor);

                        // since it is just a demo, to keep things simple, we assign all the
                        // executors into one slot.
                        cluster.assign(availableSlots.get(0), topology.getId(), executors);
                        LOG.info("We assigned executors:" + executors + " to slot: [" + availableSlots.get(0).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
                    } else {
                    	LOG.info("There is no supervisor named special-supervisor!!!");
                    }
                }
            }
        }
        
        // let system's even scheduler handle the rest scheduling work
        // you can also use your own other scheduler here, this is what
        // makes storm's scheduler composable.
        new EvenScheduler().schedule(topologies, cluster);
    }

}