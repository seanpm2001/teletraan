package com.pinterest.teletraan.worker;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.deployservice.ServiceContext;
import com.pinterest.deployservice.bean.HostBean;
import com.pinterest.deployservice.dao.DeployDAO;
import com.pinterest.deployservice.dao.HostAgentDAO;
import com.pinterest.deployservice.dao.HostDAO;
import com.pinterest.deployservice.metrics.HostClassifier;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;

public class MetricsEmitter implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsEmitter.class);

  static final String HOSTS_TOTAL = "hosts.total";
  static final String DEPLOYS_TODAY_TOTAL = "deploys.today.total";
  static final String DEPLOYS_RUNNING_TOTAL = "deploys.running.total";
  static final String HOSTS_LAUNCHING = "autoscaling.%s.launching";
  static final int TIMEOUT_THRESHOLD = 120;

  private HostClassifier hostClassifier;
  private HostDAO hostDAO;
  private Map<String, LongTaskTimer.Sample> hostTimers;

  public MetricsEmitter(ServiceContext serviceContext) {
    // HostAgentDAO is more efficient than HostDAO to get total hosts
    Gauge.builder(HOSTS_TOTAL, serviceContext.getHostAgentDAO(), MetricsEmitter::reportHostsCount)
        .strongReference(true)
        .register(Metrics.globalRegistry);

    Gauge.builder(DEPLOYS_TODAY_TOTAL, serviceContext.getDeployDAO(), MetricsEmitter::reportDailyDeployCount)
        .strongReference(true)
        .register(Metrics.globalRegistry);
    Gauge.builder(DEPLOYS_RUNNING_TOTAL, serviceContext.getDeployDAO(), MetricsEmitter::reportRunningDeployCount)
        .strongReference(true)
        .register(Metrics.globalRegistry);
  }

  @Override
  public void run() {
    try {
      List<HostBean> agentlessHosts = hostDAO.getAgentlessHosts(0, 0);
      hostClassifier.updateClassification(agentlessHosts);

      Set<HostBean> timeoutHosts = hostClassifier.getTimeoutHosts();
      for (HostBean host : timeoutHosts) {
        String hostId = host.getHost_id();
        if (hostTimers.containsKey(hostId)) {
          LongTaskTimer.Sample sample = hostTimers.get(hostId);
          sample.stop();
          hostTimers.remove(hostId);
          // failure ++;
        } else {
          // why?
        }
      }

      Set<HostBean> initializedHosts = hostClassifier.getInitializedHosts();
      for (HostBean host : initializedHosts) {
        String hostId = host.getHost_id();

        if (hostTimers.containsKey(hostId)) {
          LongTaskTimer.Sample sample = hostTimers.get(hostId);
          sample.stop();
          hostTimers.remove(hostId);
          // success ++;
        } else {
          // shouldn't happen
        }
      }

      Set<HostBean> newHosts = hostClassifier.getNewHosts();
      for (HostBean host : newHosts) {
        String timerName = String.format(HOSTS_LAUNCHING, host.getGroup_name());
        // hostTimers.put(host.getHost_id(), Metrics.more().longTaskTimer(timerName, Tags.empty()).start(host.getCreate_date()));
      }

      // stop timers for hosts that have been initializing for too long
      for (String hostId : hostTimers.keySet()) {
        LongTaskTimer.Sample sample = hostTimers.get(hostId);
        if (sample.duration(TimeUnit.MINUTES) > TIMEOUT_THRESHOLD) {
          sample.stop();
          hostTimers.remove(hostId);
        }
      }

    } catch (SQLException e) {
      LOG.error("Failed to get agentless hosts", e);
    }
  }

  static long reportHostsCount(HostAgentDAO hostAgentDAO) {
    try {
      return hostAgentDAO.getDistinctHostsCount();
    } catch (SQLException e) {
      LOG.error("Failed to get host count", e);
    }
    return 0;
  }

  static long reportDailyDeployCount(DeployDAO deployDAO) {
    try {
      return deployDAO.getDailyDeployCount();
    } catch (SQLException e) {
      LOG.error("Failed to get daily deploy count", e);
    }
    return 0;
  }

  static long reportRunningDeployCount(DeployDAO deployDAO) {
    try {
      return deployDAO.getRunningDeployCount();
    } catch (SQLException e) {
      LOG.error("Failed to get running deploy count", e);
    }
    return 0;
  }
}
