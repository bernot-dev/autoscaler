/*
Copyright 2020 The Kubernetes Authors.

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

package vpa

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/controller_fetcher"
	target_mock "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/mock"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

func parseLabelSelector(selector string) labels.Selector {
	labelSelector, _ := metav1.ParseToLabelSelector(selector)
	parsedSelector, _ := metav1.LabelSelectorAsSelector(labelSelector)
	return parsedSelector
}

func TestGetMatchingVpa(t *testing.T) {
	sts := appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StatefulSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sts",
			Namespace: "default",
		},
	}
	targetRef := &autoscalingv1.CrossVersionObjectReference{
		Kind:       sts.Kind,
		Name:       sts.Name,
		APIVersion: sts.APIVersion,
	}
	targetRefWithNoMatches := &autoscalingv1.CrossVersionObjectReference{
		Kind:       "ReplicaSet",
		Name:       "rs",
		APIVersion: "apps/v1",
	}
	podBuilderWithoutCreator := test.Pod().WithName("test-pod").WithLabels(map[string]string{"app": "test"}).
		AddContainer(test.Container().WithName("i-am-container").Get())
	podBuilder := podBuilderWithoutCreator.WithCreator(&sts.ObjectMeta, &sts.TypeMeta)
	vpaBuilder := test.VerticalPodAutoscaler().WithContainer("i-am-container")
	factor := int32(1)

	testCases := []struct {
		name            string
		pod             *corev1.Pod
		vpas            []*vpa_types.VerticalPodAutoscaler
		labelSelector   string
		expectedFound   bool
		expectedVpaName string
	}{
		{
			name: "matching selector",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeRecreate).WithName("recreate-vpa").WithTargetRef(targetRef).Get(),
			},
			labelSelector:   "app = test",
			expectedFound:   true,
			expectedVpaName: "recreate-vpa",
		}, {
			name: "no matching ownerRef (orphan pod)",
			pod:  podBuilderWithoutCreator.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeRecreate).WithName("recreate-vpa").WithTargetRef(targetRef).Get(),
			},
			expectedFound: false,
		}, {
			name: "vpa without targetRef",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeRecreate).WithName("recreate-vpa").Get(),
			},
			expectedFound: false,
		}, {
			name: "no vpa with matching targetRef",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeRecreate).WithName("recreate-vpa").WithTargetRef(targetRefWithNoMatches).Get(),
			},
			expectedFound: false,
		}, {
			name: "not matching selector",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeRecreate).WithName("recreate-vpa").WithTargetRef(targetRef).Get(),
			},
			labelSelector: "app = differentApp",
			expectedFound: false,
		}, {
			name: "off mode",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeOff).WithName("off-vpa").WithTargetRef(targetRef).Get(),
			},
			expectedFound: false,
		}, {
			name: "two vpas, one in off mode",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeOff).WithName("off-vpa").WithCreationTimestamp(time.Now().Add(-time.Hour)).WithTargetRef(targetRef).Get(),
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeRecreate).WithName("recreate-vpa").WithCreationTimestamp(time.Now()).WithTargetRef(targetRef).Get(),
			},
			labelSelector:   "app = test",
			expectedFound:   true,
			expectedVpaName: "recreate-vpa",
		}, {
			name: "initial mode",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithUpdateMode(vpa_types.UpdateModeInitial).WithName("initial-vpa").WithTargetRef(targetRef).Get(),
			},
			labelSelector:   "app = test",
			expectedFound:   true,
			expectedVpaName: "initial-vpa",
		}, {
			name:          "no vpa objects",
			pod:           podBuilder.Get(),
			vpas:          []*vpa_types.VerticalPodAutoscaler{},
			expectedFound: false,
		}, {
			name: "vpa with update mode off but with startup boost is matched",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithName("off-with-boost-vpa").WithTargetRef(targetRef).WithUpdateMode(vpa_types.UpdateModeOff).WithCPUStartupBoost(vpa_types.FactorStartupBoostType, &factor, nil, 0).Get(),
			},
			labelSelector:   "app = test",
			expectedFound:   true,
			expectedVpaName: "off-with-boost-vpa",
			// Tests that UpdateModeOff is bypassed if the VPA has a pod-level startup boost defined.
		}, {
			name: "vpa with update mode off but with container-level startup boost is matched",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithName("off-with-container-boost-vpa").WithTargetRef(targetRef).WithUpdateMode(vpa_types.UpdateModeOff).WithContainerCPUStartupBoost("i-am-container", vpa_types.FactorStartupBoostType, &factor, nil, 0).Get(),
			},
			labelSelector:   "app = test",
			expectedFound:   true,
			expectedVpaName: "off-with-container-boost-vpa",
			// Tests that UpdateModeOff is bypassed if the VPA has only a container-level startup boost defined.
		}, {
			name: "vpa with default update mode and startup boost is matched",
			pod:  podBuilder.Get(),
			vpas: []*vpa_types.VerticalPodAutoscaler{
				vpaBuilder.WithName("auto-with-boost-vpa").WithTargetRef(targetRef).WithCPUStartupBoost(vpa_types.FactorStartupBoostType, &factor, nil, 0).Get(),
			},
			labelSelector:   "app = test",
			expectedFound:   true,
			expectedVpaName: "auto-with-boost-vpa",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)

			vpaIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, vpa_api_util.VPAIndexers())
			for _, vpa := range tc.vpas {
				assert.NoError(t, vpaIndexer.Add(vpa))
			}

			if tc.labelSelector != "" {
				mockSelectorFetcher.EXPECT().Fetch(gomock.Any()).AnyTimes().Return(parseLabelSelector(tc.labelSelector), nil)
			}
			// This test is using a FakeControllerFetcher which returns the same ownerRef that is passed to it.
			// In other words, it cannot go through the hierarchy of controllers like "ReplicaSet => Deployment"
			// For this reason we are using "StatefulSet" as the ownerRef kind in the test, since it is a direct link.
			// The hierarchy part is being test in the "TestControllerFetcher" test.
			matcher := NewMatcher(vpaIndexer, mockSelectorFetcher, controllerfetcher.FakeControllerFetcher{})

			vpa := matcher.GetMatchingVPA(context.Background(), tc.pod)
			if tc.expectedFound && assert.NotNil(t, vpa) {
				assert.Equal(t, tc.expectedVpaName, vpa.Name)
			} else {
				assert.Nil(t, vpa)
			}
		})
	}
}

func TestGetMatchingVpaWithNodeSelector(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.VPANodeSelector, true)

	ds := appsv1.DaemonSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "DaemonSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ds-agent",
			Namespace: "default",
		},
	}
	targetRef := &autoscalingv1.CrossVersionObjectReference{
		Kind:       ds.Kind,
		Name:       ds.Name,
		APIVersion: ds.APIVersion,
	}

	nodeIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	assert.NoError(t, nodeIndexer.Add(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-gpu", Labels: map[string]string{"pool": "gpu"}},
	}))
	assert.NoError(t, nodeIndexer.Add(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-maint", Labels: map[string]string{"pool": "maint"}},
	}))
	assert.NoError(t, nodeIndexer.Add(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-cpu", Labels: map[string]string{"pool": "cpu"}},
	}))
	nodeLister := listersv1.NewNodeLister(nodeIndexer)

	dsPodForNode := func(nodeName string) *corev1.Pod {
		b := test.Pod().WithName("pod-" + nodeName).WithLabels(map[string]string{"app": "agent"}).
			AddContainer(test.Container().WithName("c1").Get()).
			WithCreator(&ds.ObjectMeta, &ds.TypeMeta)
		if nodeName != "" {
			b = b.WithAffinity(&corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchFields: []corev1.NodeSelectorRequirement{
									{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{nodeName}},
								},
							},
						},
					},
				},
			})
		}
		return b.Get()
	}

	vpaDryRunOff := test.VerticalPodAutoscaler().WithContainer("c1").
		WithName("vpa-dryrun-off").WithCreationTimestamp(time.Unix(1, 0)).
		WithUpdateMode(vpa_types.UpdateModeOff).WithTargetRef(targetRef).Get()
	vpaFallback := test.VerticalPodAutoscaler().WithContainer("c1").
		WithName("vpa-fallback").WithCreationTimestamp(time.Unix(2, 0)).
		WithUpdateMode(vpa_types.UpdateModeRecreate).WithTargetRef(targetRef).Get()
	vpaGPU := test.VerticalPodAutoscaler().WithContainer("c1").
		WithName("vpa-gpu").WithCreationTimestamp(time.Unix(10, 0)).
		WithUpdateMode(vpa_types.UpdateModeRecreate).WithTargetRef(targetRef).
		WithNodeSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": "gpu"}}).Get()
	vpaMaintOff := test.VerticalPodAutoscaler().WithContainer("c1").
		WithName("vpa-maint-off").WithCreationTimestamp(time.Unix(20, 0)).
		WithUpdateMode(vpa_types.UpdateModeOff).WithTargetRef(targetRef).
		WithNodeSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": "maint"}}).Get()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)
	mockSelectorFetcher.EXPECT().Fetch(gomock.Any()).AnyTimes().Return(parseLabelSelector("app = agent"), nil)

	vpaIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, vpa_api_util.VPAIndexers())
	for _, vpa := range []*vpa_types.VerticalPodAutoscaler{vpaDryRunOff, vpaFallback, vpaGPU, vpaMaintOff} {
		assert.NoError(t, vpaIndexer.Add(vpa))
	}

	matcher := NewMatcherWithNodeLister(vpaIndexer, mockSelectorFetcher, controllerfetcher.FakeControllerFetcher{}, nodeLister)

	// GPU node matches vpa-gpu despite older fallback VPA.
	gotGPU := matcher.GetMatchingVPA(context.Background(), dsPodForNode("node-gpu"))
	if assert.NotNil(t, gotGPU) {
		assert.Equal(t, "vpa-gpu", gotGPU.Name)
	}

	// Maintenance node matches Off selector VPA, which suppresses fallback VPA mutation.
	assert.Nil(t, matcher.GetMatchingVPA(context.Background(), dsPodForNode("node-maint")))

	// CPU node is uncovered by selector VPAs and falls back to vpa-fallback (ignoring older plain Off VPA).
	gotCPU := matcher.GetMatchingVPA(context.Background(), dsPodForNode("node-cpu"))
	if assert.NotNil(t, gotCPU) {
		assert.Equal(t, "vpa-fallback", gotCPU.Name)
	}

	// Missing node in cache does NOT fall back to vpa-fallback when selector VPAs target the DaemonSet.
	assert.Nil(t, matcher.GetMatchingVPA(context.Background(), dsPodForNode("node-missing")))

	// Missing nodeAffinity on DaemonSet pod does NOT fall back to vpa-fallback.
	assert.Nil(t, matcher.GetMatchingVPA(context.Background(), dsPodForNode("")))
}
