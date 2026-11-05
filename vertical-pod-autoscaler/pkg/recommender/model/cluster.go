/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package model

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/controller_fetcher"
	vpa_utils "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

const (
	// RecommendationMissingMaxDuration is maximum time that we accept the recommendation can be missing.
	RecommendationMissingMaxDuration = 30 * time.Minute
)

// ClusterState holds all runtime information about the cluster required for the
// VPA operations, i.e. configuration of resources (pods, containers,
// VPA objects), aggregated utilization of compute resources (CPU, memory) and
// events (container OOMs).
// All input to the VPA Recommender algorithm lives in this structure.
type ClusterState interface {
	StateMapSize() int
	AddOrUpdatePod(podID PodID, newLabels labels.Set, phase corev1.PodPhase)
	AddOrUpdatePodWithNode(podID PodID, newLabels labels.Set, phase corev1.PodPhase, nodeName string)
	SetNodes(nodes map[string]labels.Set)
	AddOrUpdateNode(nodeName string, nodeLabels labels.Set)
	DeleteNode(nodeName string)
	UpdateSelectorOverlapConditions()
	SetInitContainers(podID PodID, initContainers []string) error
	GetContainer(containerID ContainerID) *ContainerState
	DeletePod(podID PodID)
	AddOrUpdateContainer(containerID ContainerID, request Resources) error
	AddSample(sample *ContainerUsageSampleWithKey) error
	RecordOOM(containerID ContainerID, timestamp time.Time, requestedMemory ResourceAmount) error
	AddOrUpdateVpa(apiObject *vpa_types.VerticalPodAutoscaler, selector labels.Selector) error
	DeleteVpa(vpaID VpaID) error
	MakeAggregateStateKey(pod *PodState, containerName string) AggregateStateKey
	RateLimitedGarbageCollectAggregateCollectionStates(ctx context.Context, now time.Time, controllerFetcher controllerfetcher.ControllerFetcher)
	RecordRecommendation(vpa *Vpa, now time.Time) error
	GetMatchingPods(vpa *Vpa) []PodID
	GetControllerForPodUnderVPA(ctx context.Context, pod *PodState, controllerFetcher controllerfetcher.ControllerFetcher) *controllerfetcher.ControllerKeyWithAPIVersion
	GetControllingVPA(pod *PodState) *Vpa
	VPAs() map[VpaID]*Vpa
	SetObservedVPAs([]*vpa_types.VerticalPodAutoscaler)
	ObservedVPAs() []*vpa_types.VerticalPodAutoscaler
	Pods() map[PodID]*PodState
}

type clusterState struct {
	// Pods in the cluster.
	pods map[PodID]*PodState
	// VPA objects in the cluster.
	vpas map[VpaID]*Vpa
	// Nodes in the cluster mapped to their label sets.
	nodes map[string]labels.Set
	// VPA objects in the cluster that have no recommendation mapped to the first
	// time we've noticed the recommendation missing or last time we logged
	// a warning about it.
	// TODO consider switching to a sync.Map for emptyVPAs
	emptyVPAs map[VpaID]time.Time
	// Observed VPAs. Used to check if there are updates needed.
	observedVPAs []*vpa_types.VerticalPodAutoscaler

	// All container aggregations where the usage samples are stored.
	aggregateStateMap aggregateContainerStatesMap
	// Map with all label sets used by the aggregations. It serves as a cache
	// that allows to quickly access labels.Set corresponding to a labelSetKey.
	labelSetMap labelSetMap

	lastAggregateContainerStateGC time.Time
	gcInterval                    time.Duration

	// Mutex to protect concurrent access to maps
	mutex sync.RWMutex
}

// StateMapSize is the number of pods being tracked by the VPA
func (cluster *clusterState) StateMapSize() int {
	return len(cluster.aggregateStateMap)
}

// AggregateStateKey determines the set of containers for which the usage samples
// are kept aggregated in the model.
type AggregateStateKey interface {
	Namespace() string
	ContainerName() string
	Labels() labels.Labels
	NodeName() string
}

// String representation of the labels.LabelSet. This is the value returned by
// labelSet.String(). As opposed to the LabelSet object, it can be used as a map key.
type labelSetKey string

// Map of label sets keyed by their string representation.
type labelSetMap map[labelSetKey]labels.Set

// AggregateContainerStatesMap is a map from AggregateStateKey to AggregateContainerState.
type aggregateContainerStatesMap map[AggregateStateKey]*AggregateContainerState

// PodState holds runtime information about a single Pod.
type PodState struct {
	// Unique id of the Pod.
	ID PodID
	// Set of labels attached to the Pod.
	labelSetKey labelSetKey
	// NodeName is the name of the node the Pod runs on.
	NodeName string
	// Containers that belong to the Pod, keyed by the container name.
	Containers map[string]*ContainerState
	// containerAggKeys tracks the current AggregateStateKey for each container.
	containerAggKeys map[string]AggregateStateKey
	// InitContainers is a list of init containers names which belong to the Pod.
	InitContainers []string
	// PodPhase describing current life cycle phase of the Pod.
	Phase corev1.PodPhase
}

// NewClusterState returns a new clusterState with no pods.
func NewClusterState(gcInterval time.Duration) *clusterState {
	return &clusterState{
		pods:                          make(map[PodID]*PodState),
		vpas:                          make(map[VpaID]*Vpa),
		nodes:                         make(map[string]labels.Set),
		emptyVPAs:                     make(map[VpaID]time.Time),
		aggregateStateMap:             make(aggregateContainerStatesMap),
		labelSetMap:                   make(labelSetMap),
		lastAggregateContainerStateGC: time.Unix(0, 0),
		gcInterval:                    gcInterval,
	}
}

// ContainerUsageSampleWithKey holds a ContainerUsageSample together with the
// ID of the container it belongs to.
type ContainerUsageSampleWithKey struct {
	ContainerUsageSample
	Container ContainerID
}

// AddOrUpdatePod updates the state of the pod with a given PodID, if it is
// present in the cluster object. Otherwise a new pod is created and added to
// the Cluster object.
// If the labels of the pod have changed, it updates the links between the containers
// and the aggregations.
func (cluster *clusterState) AddOrUpdatePod(podID PodID, newLabels labels.Set, phase corev1.PodPhase) {
	nodeName := ""
	if existing, ok := cluster.pods[podID]; ok {
		nodeName = existing.NodeName
	}
	cluster.AddOrUpdatePodWithNode(podID, newLabels, phase, nodeName)
}

// AddOrUpdatePodWithNode updates the state of the pod including the node it runs on.
func (cluster *clusterState) AddOrUpdatePodWithNode(podID PodID, newLabels labels.Set, phase corev1.PodPhase, nodeName string) {
	pod, podExists := cluster.pods[podID]
	if !podExists {
		pod = newPod(podID)
		cluster.pods[podID] = pod
	}

	newlabelSetKey := cluster.getLabelSetKey(newLabels)
	labelsOrNodeChanged := pod.labelSetKey != newlabelSetKey || pod.NodeName != nodeName
	if podExists && labelsOrNodeChanged {
		// This Pod is already counted in the old VPA, remove the link.
		cluster.removePodFromItsVpa(pod)
	}
	if !podExists || labelsOrNodeChanged {
		pod.labelSetKey = newlabelSetKey
		pod.NodeName = nodeName
		if len(pod.Containers) > 0 {
			cluster.reconcilePodAggregations()
		}
		// Ensure aggregations exist for current pod keys while keeping ContainerStateAggregatorProxy intact.
		for containerName, container := range pod.Containers {
			containerID := ContainerID{PodID: podID, ContainerName: containerName}
			cluster.findOrCreateAggregateContainerState(containerID)
			if pod.containerAggKeys == nil {
				pod.containerAggKeys = make(map[string]AggregateStateKey)
			}
			pod.containerAggKeys[containerName] = cluster.MakeAggregateStateKey(pod, containerName)
			container.aggregator = NewContainerStateAggregatorProxy(cluster, containerID)
		}

		cluster.addPodToItsVpa(pod)
	}
	pod.Phase = phase
}

// SetInitContainers sets the names of init containers that belong to the pod.
// Requires the pod to be added to the clusterState first. Otherwise an error is
// returned.
func (cluster *clusterState) SetInitContainers(podID PodID, initContainers []string) error {
	pod, podExists := cluster.pods[podID]
	if !podExists || pod == nil {
		return NewKeyError(podID)
	}
	pod.InitContainers = initContainers
	return nil
}

func (cluster *clusterState) getNodeLabels(nodeName string) (labels.Set, bool) {
	nLabels, ok := cluster.nodes[nodeName]
	return nLabels, ok
}

func (cluster *clusterState) podMatchesVPA(pod *PodState, vpa *Vpa) bool {
	if !vpa_utils.PodLabelsMatchVPA(pod.ID.Namespace, cluster.labelSetMap[pod.labelSetKey], vpa.ID.Namespace, vpa.PodSelector) {
		return false
	}
	if features.Enabled(features.VPANodeSelector) && vpa.HasSelector {
		if pod.NodeName == "" || vpa.NodeSelector == nil {
			return false
		}
		nLabels, ok := cluster.nodes[pod.NodeName]
		if !ok {
			return false
		}
		return vpa.NodeSelector.Matches(nLabels)
	}
	return true
}

// addPodToItsVpa increases the count of Pods associated with a VPA object.
// Does a scan similar to findOrCreateAggregateContainerState so could be optimized if needed.
func (cluster *clusterState) addPodToItsVpa(pod *PodState) {
	for _, vpa := range cluster.vpas {
		if cluster.podMatchesVPA(pod, vpa) {
			vpa.PodCount++
		}
	}
}

// removePodFromItsVpa decreases the count of Pods associated with a VPA object.
func (cluster *clusterState) removePodFromItsVpa(pod *PodState) {
	for _, vpa := range cluster.vpas {
		if cluster.podMatchesVPA(pod, vpa) {
			vpa.PodCount--
		}
	}
}

// GetContainer returns the ContainerState object for a given ContainerID or
// null if it's not present in the model.
func (cluster *clusterState) GetContainer(containerID ContainerID) *ContainerState {
	pod, podExists := cluster.pods[containerID.PodID]
	if podExists {
		container, containerExists := pod.Containers[containerID.ContainerName]
		if containerExists {
			return container
		}
	}
	return nil
}

// DeletePod removes an existing pod from the cluster.
func (cluster *clusterState) DeletePod(podID PodID) {
	pod, found := cluster.pods[podID]
	if found {
		cluster.removePodFromItsVpa(pod)
	}
	delete(cluster.pods, podID)
}

// AddOrUpdateContainer creates a new container with the given ContainerID and
// adds it to the parent pod in the clusterState object, if not yet present.
// Requires the pod to be added to the clusterState first. Otherwise an error is
// returned.
func (cluster *clusterState) AddOrUpdateContainer(containerID ContainerID, request Resources) error {
	pod, podExists := cluster.pods[containerID.PodID]
	if !podExists {
		return NewKeyError(containerID.PodID)
	}
	if container, containerExists := pod.Containers[containerID.ContainerName]; !containerExists {
		cluster.findOrCreateAggregateContainerState(containerID)
		if pod.containerAggKeys == nil {
			pod.containerAggKeys = make(map[string]AggregateStateKey)
		}
		pod.containerAggKeys[containerID.ContainerName] = cluster.MakeAggregateStateKey(pod, containerID.ContainerName)
		pod.Containers[containerID.ContainerName] = NewContainerState(request, NewContainerStateAggregatorProxy(cluster, containerID))
	} else {
		// Container aleady exists. Possibly update the request.
		container.Request = request
	}
	return nil
}

// AddSample adds a new usage sample to the proper container in the clusterState
// object. Requires the container as well as the parent pod to be added to the
// clusterState first. Otherwise an error is returned.
func (cluster *clusterState) AddSample(sample *ContainerUsageSampleWithKey) error {
	pod, podExists := cluster.pods[sample.Container.PodID]
	if !podExists {
		return NewKeyError(sample.Container.PodID)
	}
	containerState, containerExists := pod.Containers[sample.Container.ContainerName]
	if !containerExists {
		return NewKeyError(sample.Container)
	}
	if !containerState.AddSample(&sample.ContainerUsageSample) {
		return errors.New("sample discarded (invalid or out of order)")
	}
	return nil
}

// RecordOOM adds info regarding OOM event in the model as an artificial memory sample.
func (cluster *clusterState) RecordOOM(containerID ContainerID, timestamp time.Time, requestedMemory ResourceAmount) error {
	pod, podExists := cluster.pods[containerID.PodID]
	if !podExists {
		return NewKeyError(containerID.PodID)
	}
	containerState, containerExists := pod.Containers[containerID.ContainerName]
	if !containerExists {
		return NewKeyError(containerID.ContainerName)
	}
	err := containerState.RecordOOM(timestamp, requestedMemory)
	if err != nil {
		return fmt.Errorf("error while recording OOM for %v, Reason: %v", containerID, err)
	}
	return nil
}

// AddOrUpdateVpa adds a new VPA with a given ID to the clusterState if it
// didn't yet exist. If the VPA already existed but had a different pod
// selector, the pod selector is updated. Updates the links between the VPA and
// all aggregations it matches.
func (cluster *clusterState) AddOrUpdateVpa(apiObject *vpa_types.VerticalPodAutoscaler, selector labels.Selector) error {
	vpaID := VpaID{Namespace: apiObject.Namespace, VpaName: apiObject.Name}
	annotationsMap := apiObject.Annotations
	conditionsMap := make(vpaConditionsMap)
	for _, condition := range apiObject.Status.Conditions {
		conditionsMap[condition.Type] = condition
	}
	var currentRecommendation *vpa_types.RecommendedPodResources
	if conditionsMap[vpa_types.RecommendationProvided].Status == corev1.ConditionTrue {
		currentRecommendation = apiObject.Status.Recommendation
	}

	var hasNodeSelector bool
	var nodeSelector labels.Selector
	if features.Enabled(features.VPANodeSelector) && vpa_utils.HasSelector(apiObject) {
		hasNodeSelector = true
		parsed, err := metav1.LabelSelectorAsSelector(apiObject.Spec.Selector.NodeSelector)
		if err != nil {
			klog.ErrorS(err, "Invalid nodeSelector on VPA", "vpa", klog.KObj(apiObject))
			nodeSelector = labels.Nothing()
		} else {
			nodeSelector = parsed
		}
	}

	vpa, vpaExists := cluster.vpas[vpaID]
	if vpaExists && (vpa.PodSelector.String() != selector.String()) {
		// Pod selector was changed. Delete the VPA object and recreate
		// it with the new selector.
		if err := cluster.DeleteVpa(vpaID); err != nil {
			return err
		}
		vpaExists = false
	}

	nodeSelectorChanged := false
	if vpaExists {
		oldNS := ""
		if vpa.NodeSelector != nil {
			oldNS = vpa.NodeSelector.String()
		}
		newNS := ""
		if nodeSelector != nil {
			newNS = nodeSelector.String()
		}
		nodeSelectorChanged = vpa.HasSelector != hasNodeSelector || oldNS != newNS
	}

	if !vpaExists {
		vpa = NewVpa(vpaID, selector, apiObject.CreationTimestamp.Time)
		cluster.vpas[vpaID] = vpa
	}
	vpa.TargetRef = apiObject.Spec.TargetRef
	vpa.HasSelector = hasNodeSelector
	vpa.NodeSelector = nodeSelector
	vpa.nodeLabels = cluster.getNodeLabels
	vpa.Annotations = annotationsMap
	vpa.SetConditionsMap(conditionsMap)
	vpa.SetRecommendationDirect(currentRecommendation)
	vpa.SetUpdateMode(apiObject.Spec.UpdatePolicy)
	vpa.SetResourcePolicy(apiObject.Spec.ResourcePolicy)
	vpa.SetAPIVersion(apiObject.GetObjectKind().GroupVersionKind().Version)
	vpa.Generation = apiObject.Generation

	if !vpaExists || nodeSelectorChanged {
		if features.Enabled(features.VPANodeSelector) {
			cluster.reconcilePodAggregations()
		}
		for aggregationKey, aggregation := range cluster.aggregateStateMap {
			if vpa.matchesAggregation(aggregationKey) {
				vpa.UseAggregationIfMatching(aggregationKey, aggregation)
			} else if vpa.UsesAggregation(aggregationKey) {
				vpa.DeleteAggregation(aggregationKey)
				cluster.refreshAggregationController(aggregationKey, aggregation)
			}
		}
		if features.Enabled(features.VPANodeSelector) {
			cluster.refreshAllAggregationControllers()
			cluster.recomputePodCounts()
		} else {
			vpa.PodCount = len(cluster.GetMatchingPods(vpa))
		}
	}
	return nil
}

// DeleteVpa removes a VPA with the given ID from the clusterState.
func (cluster *clusterState) DeleteVpa(vpaID VpaID) error {
	vpa, vpaExists := cluster.vpas[vpaID]
	if !vpaExists {
		return NewKeyError(vpaID)
	}
	wasSelectorVPA := vpa.HasSelector
	for key, state := range vpa.aggregateContainerStates {
		if !features.Enabled(features.VPANodeSelector) || state.releaseControl(vpa) {
			state.MarkNotAutoscaled()
		}
		delete(vpa.aggregateContainerStates, key)
	}
	delete(cluster.vpas, vpaID)
	if features.Enabled(features.VPANodeSelector) {
		if wasSelectorVPA {
			cluster.reconcilePodAggregations()
		}
		cluster.refreshAllAggregationControllers()
		cluster.recomputePodCounts()
	}
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	delete(cluster.emptyVPAs, vpaID)
	return nil
}

func (cluster *clusterState) VPAs() map[VpaID]*Vpa {
	return cluster.vpas
}

func (cluster *clusterState) Pods() map[PodID]*PodState {
	return cluster.pods
}

func (cluster *clusterState) SetObservedVPAs(observedVPAs []*vpa_types.VerticalPodAutoscaler) {
	cluster.observedVPAs = observedVPAs
}

func (cluster *clusterState) ObservedVPAs() []*vpa_types.VerticalPodAutoscaler {
	return cluster.observedVPAs
}

func newPod(id PodID) *PodState {
	return &PodState{
		ID:               id,
		Containers:       make(map[string]*ContainerState),
		containerAggKeys: make(map[string]AggregateStateKey),
	}
}

func (cluster *clusterState) shouldKeyByNode(pod *PodState) bool {
	if !features.Enabled(features.VPANodeSelector) || pod == nil || pod.NodeName == "" {
		return false
	}
	podLabels := cluster.labelSetMap[pod.labelSetKey]
	for _, vpa := range cluster.vpas {
		if !vpa.HasSelector || vpa.TargetRef == nil || vpa.TargetRef.Kind != "DaemonSet" {
			continue
		}
		if vpa_utils.PodLabelsMatchVPA(pod.ID.Namespace, podLabels, vpa.ID.Namespace, vpa.PodSelector) {
			return true
		}
	}
	return false
}

// SetNodes updates the full map of cluster nodes and their labels, refreshing
// node-scoped VPA aggregation links and pod counts when node labels change.
func (cluster *clusterState) SetNodes(nodes map[string]labels.Set) {
	changed := len(nodes) != len(cluster.nodes)
	if !changed {
		for name, newSet := range nodes {
			oldSet, ok := cluster.nodes[name]
			if !ok || !maps.Equal(oldSet, newSet) {
				changed = true
				break
			}
		}
	}
	if !changed {
		return
	}
	next := make(map[string]labels.Set, len(nodes))
	for name, set := range nodes {
		cloned := make(labels.Set, len(set))
		maps.Copy(cloned, set)
		next[name] = cloned
	}
	cluster.nodes = next
	cluster.onNodesChanged()
}

// AddOrUpdateNode adds or updates a single node's labels in ClusterState.
func (cluster *clusterState) AddOrUpdateNode(nodeName string, nodeLabels labels.Set) {
	cloned := make(labels.Set, len(nodeLabels))
	maps.Copy(cloned, nodeLabels)
	if old, ok := cluster.nodes[nodeName]; ok && maps.Equal(old, cloned) {
		return
	}
	cluster.nodes[nodeName] = cloned
	cluster.onNodesChanged()
}

// DeleteNode removes a node from ClusterState.
func (cluster *clusterState) DeleteNode(nodeName string) {
	if _, ok := cluster.nodes[nodeName]; !ok {
		return
	}
	delete(cluster.nodes, nodeName)
	cluster.onNodesChanged()
}

func (cluster *clusterState) onNodesChanged() {
	if !features.Enabled(features.VPANodeSelector) {
		return
	}
	for aggKey, agg := range cluster.aggregateStateMap {
		if aggKey.NodeName() == "" {
			continue
		}
		for _, vpa := range cluster.vpas {
			if !vpa.HasSelector {
				continue
			}
			if vpa.matchesAggregation(aggKey) {
				vpa.UseAggregationIfMatching(aggKey, agg)
			} else if vpa.UsesAggregation(aggKey) {
				vpa.DeleteAggregation(aggKey)
			}
		}
		cluster.refreshAggregationController(aggKey, agg)
	}
	cluster.recomputePodCounts()
}

func (cluster *clusterState) recomputePodCounts() {
	for _, vpa := range cluster.vpas {
		vpa.PodCount = len(cluster.GetMatchingPods(vpa))
	}
}

func (cluster *clusterState) refreshAggregationController(key AggregateStateKey, state *AggregateContainerState) {
	if !features.Enabled(features.VPANodeSelector) {
		return
	}
	var best *Vpa
	for _, vpa := range cluster.vpas {
		if vpa.UsesAggregation(key) && isVpaStronger(vpa, best) {
			best = vpa
		}
	}
	if best == nil {
		state.setControllingVpa(nil)
		state.MarkNotAutoscaled()
		return
	}
	state.setControllingVpa(best)
	state.IsUnderVPA = true
	state.UpdateMode = best.UpdateMode
	state.UpdateFromPolicy(vpa_utils.GetContainerResourcePolicy(key.ContainerName(), best.ResourcePolicy))
	if rec := best.GetRecommendation(); rec != nil {
		for _, cr := range rec.ContainerRecommendations {
			if cr.ContainerName == key.ContainerName() {
				state.SetLastRecommendation(cr.UncappedTarget)
				break
			}
		}
	}
}

func (cluster *clusterState) refreshAllAggregationControllers() {
	for key, state := range cluster.aggregateStateMap {
		cluster.refreshAggregationController(key, state)
	}
}

func (cluster *clusterState) findOrCreateAggregateContainerStateByKey(aggregateStateKey AggregateStateKey) *AggregateContainerState {
	aggregateContainerState, aggregateStateExists := cluster.aggregateStateMap[aggregateStateKey]
	if !aggregateStateExists {
		aggregateContainerState = NewAggregateContainerState()
		cluster.aggregateStateMap[aggregateStateKey] = aggregateContainerState
		for _, vpa := range cluster.vpas {
			vpa.UseAggregationIfMatching(aggregateStateKey, aggregateContainerState)
		}
	}
	return aggregateContainerState
}

func (cluster *clusterState) anyContainerUsesKey(target AggregateStateKey) bool {
	for _, pod := range cluster.pods {
		for _, key := range pod.containerAggKeys {
			if key == target {
				return true
			}
		}
	}
	return false
}

// reconcilePodAggregations handles live re-keying of pod container aggregations when
// the first selector VPA for a DaemonSet is created (0 -> 1) or the last one is deleted
// (1 -> 0), or when a pod's labels or node change.
func (cluster *clusterState) reconcilePodAggregations() {
	mergedOldKeys := make(map[AggregateStateKey]bool)
	candidateEmptyOldKeys := make(map[AggregateStateKey]bool)

	for _, pod := range cluster.pods {
		if pod.containerAggKeys == nil {
			pod.containerAggKeys = make(map[string]AggregateStateKey)
		}
		for containerName, container := range pod.Containers {
			newKey := cluster.MakeAggregateStateKey(pod, containerName)
			oldKey, hasOldKey := pod.containerAggKeys[containerName]
			if !hasOldKey {
				cluster.findOrCreateAggregateContainerStateByKey(newKey)
				pod.containerAggKeys[containerName] = newKey
				continue
			}
			if oldKey == newKey {
				continue
			}
			if oldKey.NodeName() != "" && newKey.NodeName() == "" {
				// 1 -> 0 selector VPAs: merge per-node aggregations back into the shared DaemonSet aggregation.
				sharedAgg := cluster.findOrCreateAggregateContainerStateByKey(newKey)
				if !mergedOldKeys[oldKey] {
					mergedOldKeys[oldKey] = true
					if oldAgg, ok := cluster.aggregateStateMap[oldKey]; ok {
						sharedAgg.MergeContainerState(oldAgg)
						delete(cluster.aggregateStateMap, oldKey)
						for _, vpa := range cluster.vpas {
							vpa.DeleteAggregation(oldKey)
						}
					}
				}
				pod.containerAggKeys[containerName] = newKey
				continue
			}

			// 0 -> 1 selector VPA split (or label/node update): create new aggregation and migrate ongoing memory peak.
			newAgg := cluster.findOrCreateAggregateContainerStateByKey(newKey)
			if oldAgg, ok := cluster.aggregateStateMap[oldKey]; ok {
				if !container.WindowEnd.IsZero() && container.GetMaxMemoryPeak() != 0 {
					peak := ContainerUsageSample{
						MeasureStart: container.WindowEnd,
						Usage:        container.GetMaxMemoryPeak(),
						Resource:     ResourceMemory,
					}
					oldAgg.SubtractSample(&peak)
					newAgg.AddSample(&peak)
				}
				candidateEmptyOldKeys[oldKey] = true
			}
			pod.containerAggKeys[containerName] = newKey
		}
	}

	for oldKey := range candidateEmptyOldKeys {
		if oldAgg, ok := cluster.aggregateStateMap[oldKey]; ok {
			if !cluster.anyContainerUsesKey(oldKey) && oldAgg.isEmpty() && oldAgg.AggregateMemoryPeaks.IsEmpty() {
				delete(cluster.aggregateStateMap, oldKey)
				for _, vpa := range cluster.vpas {
					vpa.DeleteAggregation(oldKey)
				}
			}
		}
	}
}

// UpdateSelectorOverlapConditions evaluates overlapping selector VPAs targeting the
// same controller and sets or clears the SelectorOverlap condition accordingly.
func (cluster *clusterState) UpdateSelectorOverlapConditions() {
	if !features.Enabled(features.VPANodeSelector) {
		for _, vpa := range cluster.vpas {
			vpa.DeleteCondition(vpa_types.SelectorOverlap)
		}
		return
	}

	type targetKey struct {
		namespace string
		kind      string
		name      string
	}
	byTarget := make(map[targetKey][]*Vpa)
	for _, vpa := range cluster.vpas {
		if !vpa.HasSelector || vpa.NodeSelector == nil || vpa.TargetRef == nil || vpa.PodSelector == nil {
			vpa.DeleteCondition(vpa_types.SelectorOverlap)
			continue
		}
		tk := targetKey{
			namespace: vpa.ID.Namespace,
			kind:      vpa.TargetRef.Kind,
			name:      vpa.TargetRef.Name,
		}
		byTarget[tk] = append(byTarget[tk], vpa)
	}

	for _, selectorVPAs := range byTarget {
		if len(selectorVPAs) <= 1 {
			for _, vpa := range selectorVPAs {
				vpa.DeleteCondition(vpa_types.SelectorOverlap)
			}
			continue
		}

		// Collect sorted unique node names running an active pod of this target controller.
		nodeSet := make(map[string]bool)
		rep := selectorVPAs[0]
		for _, pod := range cluster.pods {
			if pod.NodeName == "" || pod.Phase == corev1.PodSucceeded || pod.Phase == corev1.PodFailed {
				continue
			}
			if _, nodeKnown := cluster.nodes[pod.NodeName]; !nodeKnown {
				continue
			}
			if vpa_utils.PodLabelsMatchVPA(pod.ID.Namespace, cluster.labelSetMap[pod.labelSetKey], rep.ID.Namespace, rep.PodSelector) {
				nodeSet[pod.NodeName] = true
			}
		}
		podNodes := make([]string, 0, len(nodeSet))
		for n := range nodeSet {
			podNodes = append(podNodes, n)
		}
		slices.Sort(podNodes)

		for _, v := range selectorVPAs {
			var matchedNodes []string
			for _, nodeName := range podNodes {
				if v.NodeSelector.Matches(cluster.nodes[nodeName]) {
					matchedNodes = append(matchedNodes, nodeName)
				}
			}
			lostByWinner := make(map[VpaID][]string)
			lostCount := 0
			for _, nodeName := range matchedNodes {
				nodeLabels := cluster.nodes[nodeName]
				var winner *Vpa
				for _, other := range selectorVPAs {
					if other.NodeSelector.Matches(nodeLabels) && isVpaStronger(other, winner) {
						winner = other
					}
				}
				if winner != nil && winner.ID != v.ID {
					lostByWinner[winner.ID] = append(lostByWinner[winner.ID], nodeName)
					lostCount++
				}
			}
			if lostCount == 0 {
				v.DeleteCondition(vpa_types.SelectorOverlap)
				continue
			}
			winnerIDs := make([]VpaID, 0, len(lostByWinner))
			for wID := range lostByWinner {
				winnerIDs = append(winnerIDs, wID)
			}
			slices.SortFunc(winnerIDs, func(a, b VpaID) int {
				if c := cmp.Compare(a.Namespace, b.Namespace); c != 0 {
					return c
				}
				return cmp.Compare(a.VpaName, b.VpaName)
			})
			parts := make([]string, 0, len(winnerIDs))
			for _, wID := range winnerIDs {
				parts = append(parts, fmt.Sprintf("VPA %s/%s: %s", wID.Namespace, wID.VpaName, strings.Join(lostByWinner[wID], ", ")))
			}
			verb := "are"
			if lostCount == 1 {
				verb = "is"
			}
			msg := fmt.Sprintf("%d of %d nodes %s controlled by %s", lostCount, len(matchedNodes), verb, strings.Join(parts, "; "))
			v.SetCondition(vpa_types.SelectorOverlap, true, "LostTieBreak", msg)
		}
	}
}

// getLabelSetKey puts the given labelSet in the global labelSet map and returns a
// corresponding labelSetKey.
func (cluster *clusterState) getLabelSetKey(labelSet labels.Set) labelSetKey {
	labelSetKey := labelSetKey(labelSet.String())
	cluster.labelSetMap[labelSetKey] = labelSet
	return labelSetKey
}

// MakeAggregateStateKey returns the AggregateStateKey that should be used
// to aggregate usage samples from a container with the given name in a given pod.
func (cluster *clusterState) MakeAggregateStateKey(pod *PodState, containerName string) AggregateStateKey {
	nodeName := ""
	if cluster.shouldKeyByNode(pod) {
		nodeName = pod.NodeName
	}
	return aggregateStateKey{
		namespace:     pod.ID.Namespace,
		containerName: containerName,
		labelSetKey:   pod.labelSetKey,
		labelSetMap:   &cluster.labelSetMap,
		nodeName:      nodeName,
	}
}

// aggregateStateKeyForContainerID returns the AggregateStateKey for the ContainerID.
// The pod with the corresponding PodID must already be present in the clusterState.
func (cluster *clusterState) aggregateStateKeyForContainerID(containerID ContainerID) AggregateStateKey {
	pod, podExists := cluster.pods[containerID.PodID]
	if !podExists {
		panic(fmt.Sprintf("Pod not present in the ClusterState: %s/%s", containerID.Namespace, containerID.PodName))
	}
	return cluster.MakeAggregateStateKey(pod, containerID.ContainerName)
}

// findOrCreateAggregateContainerState returns (possibly newly created) AggregateContainerState
// that should be used to aggregate usage samples from container with a given ID.
// The pod with the corresponding PodID must already be present in the clusterState.
func (cluster *clusterState) findOrCreateAggregateContainerState(containerID ContainerID) *AggregateContainerState {
	aggregateStateKey := cluster.aggregateStateKeyForContainerID(containerID)
	return cluster.findOrCreateAggregateContainerStateByKey(aggregateStateKey)
}

// garbageCollectAggregateCollectionStates removes obsolete AggregateCollectionStates from the clusterState.
// AggregateCollectionState is obsolete in following situations:
// 1) It has no samples and there are no more contributive pods - a pod is contributive in any of following situations:
//
//	a) It is in an active state - i.e. not PodSucceeded nor PodFailed.
//	b) Its associated controller (e.g. Deployment) still exists.
//
// 2) The last sample is too old to give meaningful recommendation (>8 days),
// 3) There are no samples and the aggregate state was created >8 days ago.
func (cluster *clusterState) garbageCollectAggregateCollectionStates(ctx context.Context, now time.Time, controllerFetcher controllerfetcher.ControllerFetcher) {
	klog.V(1).InfoS("Garbage collection of AggregateCollectionStates triggered")
	keysToDelete := make([]AggregateStateKey, 0)
	contributiveKeys := cluster.getContributiveAggregateStateKeys(ctx, controllerFetcher)
	for key, aggregateContainerState := range cluster.aggregateStateMap {
		isKeyContributive := contributiveKeys[key]
		if !isKeyContributive && aggregateContainerState.isEmpty() {
			keysToDelete = append(keysToDelete, key)
			klog.V(1).InfoS("Removing empty and not contributive AggregateCollectionState", "key", key)
			continue
		}
		if aggregateContainerState.isExpired(now) {
			keysToDelete = append(keysToDelete, key)
			klog.V(1).InfoS("Removing expired AggregateCollectionState", "key", key)
		}
	}
	for _, key := range keysToDelete {
		delete(cluster.aggregateStateMap, key)
		for _, vpa := range cluster.vpas {
			vpa.DeleteAggregation(key)
		}
	}
}

// RateLimitedGarbageCollectAggregateCollectionStates removes obsolete AggregateCollectionStates from the clusterState.
// It performs clean up only if more than `gcInterval` passed since the last time it performed a cleanup.
// AggregateCollectionState is obsolete in following situations:
// 1) It has no samples and there are no more contributive pods - a pod is contributive in any of following situations:
//
//	a) It is in an active state - i.e. not PodSucceeded nor PodFailed.
//	b) Its associated controller (e.g. Deployment) still exists.
//
// 2) The last sample is too old to give meaningful recommendation (>8 days),
// 3) There are no samples and the aggregate state was created >8 days ago.
func (cluster *clusterState) RateLimitedGarbageCollectAggregateCollectionStates(ctx context.Context, now time.Time, controllerFetcher controllerfetcher.ControllerFetcher) {
	if now.Sub(cluster.lastAggregateContainerStateGC) < cluster.gcInterval {
		return
	}
	cluster.garbageCollectAggregateCollectionStates(ctx, now, controllerFetcher)
	cluster.lastAggregateContainerStateGC = now
}

func (cluster *clusterState) getContributiveAggregateStateKeys(ctx context.Context, controllerFetcher controllerfetcher.ControllerFetcher) map[AggregateStateKey]bool {
	contributiveKeys := map[AggregateStateKey]bool{}
	for _, pod := range cluster.pods {
		// Pod is considered contributive in any of following situations:
		// 1) It is in active state - i.e. not PodSucceeded nor PodFailed.
		// 2) Its associated controller (e.g. Deployment) still exists.
		podControllerExists := cluster.GetControllerForPodUnderVPA(ctx, pod, controllerFetcher) != nil
		podActive := pod.Phase != corev1.PodSucceeded && pod.Phase != corev1.PodFailed
		if podActive || podControllerExists {
			for container := range pod.Containers {
				contributiveKeys[cluster.MakeAggregateStateKey(pod, container)] = true
			}
		}
	}
	return contributiveKeys
}

// RecordRecommendation marks the state of recommendation in the cluster. We
// keep track of empty recommendations and log information about them
// periodically.
func (cluster *clusterState) RecordRecommendation(vpa *Vpa, now time.Time) error {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()
	// TODO(jkyros): Today the VPA critical sections don't try to grab the cluster mutex, but
	// if anyone ever changes it so they do we'd deadlock. This makes me a little nervous, but
	// if I don't fix this one, we'll fail the race tests
	if vpa.HasRecommendation() {
		delete(cluster.emptyVPAs, vpa.ID)
		return nil
	}
	lastLogged, ok := cluster.emptyVPAs[vpa.ID]
	if !ok {
		cluster.emptyVPAs[vpa.ID] = now
	} else {
		if lastLogged.Add(RecommendationMissingMaxDuration).Before(now) {
			cluster.emptyVPAs[vpa.ID] = now
			return fmt.Errorf("VPA %s/%s is missing recommendation for more than %v", vpa.ID.Namespace, vpa.ID.VpaName, RecommendationMissingMaxDuration)
		}
	}
	return nil
}

// GetMatchingPods returns a list of currently active pods that match the
// given VPA. Traverses through all pods in the cluster - use sparingly.
func (cluster *clusterState) GetMatchingPods(vpa *Vpa) []PodID {
	matchingPods := []PodID{}
	for podID, pod := range cluster.pods {
		if cluster.podMatchesVPA(pod, vpa) {
			matchingPods = append(matchingPods, podID)
		}
	}
	return matchingPods
}

// GetControllerForPodUnderVPA returns controller associated with given Pod. Returns nil if Pod is not controlled by a VPA object.
func (cluster *clusterState) GetControllerForPodUnderVPA(ctx context.Context, pod *PodState, controllerFetcher controllerfetcher.ControllerFetcher) *controllerfetcher.ControllerKeyWithAPIVersion {
	controllingVPA := cluster.GetControllingVPA(pod)
	if controllingVPA != nil && controllingVPA.TargetRef != nil {
		controller := &controllerfetcher.ControllerKeyWithAPIVersion{
			ControllerKey: controllerfetcher.ControllerKey{
				Namespace: controllingVPA.ID.Namespace,
				Kind:      controllingVPA.TargetRef.Kind,
				Name:      controllingVPA.TargetRef.Name,
			},
			ApiVersion: controllingVPA.TargetRef.APIVersion,
		}
		topLevelController, _ := controllerFetcher.FindTopMostWellKnownOrScalable(ctx, controller)
		return topLevelController
	}
	return nil
}

// GetControllingVPA returns a VPA object controlling given Pod.
func (cluster *clusterState) GetControllingVPA(pod *PodState) *Vpa {
	var controlling *Vpa
	for _, vpa := range cluster.vpas {
		if cluster.podMatchesVPA(pod, vpa) && isVpaStronger(vpa, controlling) {
			controlling = vpa
		}
	}
	return controlling
}

// Implementation of the AggregateStateKey interface. It can be used as a map key.
type aggregateStateKey struct {
	namespace     string
	containerName string
	labelSetKey   labelSetKey
	// Pointer to the global map from labelSetKey to labels.Set.
	// Note: a pointer is used so that two copies of the same key are equal.
	labelSetMap *labelSetMap
	nodeName    string
}

// Namespace returns the namespace for the aggregateStateKey.
func (k aggregateStateKey) Namespace() string {
	return k.namespace
}

// ContainerName returns the name of the container for the aggregateStateKey.
func (k aggregateStateKey) ContainerName() string {
	return k.containerName
}

// Labels returns the set of labels for the aggregateStateKey.
func (k aggregateStateKey) Labels() labels.Labels {
	if k.labelSetMap == nil {
		return labels.Set{}
	}
	return (*k.labelSetMap)[k.labelSetKey]
}

// NodeName returns the node name for the aggregateStateKey, or "" when shared across nodes.
func (k aggregateStateKey) NodeName() string {
	return k.nodeName
}
