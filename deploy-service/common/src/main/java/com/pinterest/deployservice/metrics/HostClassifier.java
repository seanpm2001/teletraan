package com.pinterest.deployservice.metrics;

import java.util.Collection;
import java.util.Set;

import com.pinterest.deployservice.bean.HostBean;

public interface HostClassifier {

    /**
     * Retrieves hosts that have timed out.
     *
     * @return a set of hosts that have timed out
     */
    Set<HostBean> getTimeoutHosts();

    /**
     * Retrieves hosts that have been successfully initialized.
     *
     * @return a set of hosts that have been initialized
     */
    Set<HostBean> getInitializedHosts();

    /**
     * Retrieves hosts that are currently initializing.
     *
     * Note this is the union of newly added hosts and carryover hosts.
     *
     * @return a set of hosts that are currently initializing
     */
    Set<HostBean> getInitializingHosts();

    /**
     * Retrieves hosts that are newly added.
     *
     * Note this is a subset of hosts that are initializing.
     *
     * @return a set of newly added hosts
     */
    Set<HostBean> getNewHosts();

    /**
     * Retrieves hosts that are carried over from last update.
     *
     * Note this is a subset of hosts that are initializing.
     *
     * @return a set of newly added hosts
     */
    Set<HostBean> getCarryoverHosts();

    /**
     * Updates the classification of hosts.
     *
     * @param hosts the collection of hosts to update the classification with
     */
    void updateClassification(Collection<HostBean> hosts);
}