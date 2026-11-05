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

package logic

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/time/rate"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
	"k8s.io/utils/set"

	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource/pod/patch"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/admission-controller/resource/pod/recommendation"
	vpa_types "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/apis/autoscaling.k8s.io/v1"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/features"
	controllerfetcher "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/controller_fetcher"
	target_mock "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/target/mock"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/inplace"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/priority"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/restriction"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/updater/utils"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/annotations"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/limitrange"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/status"
	"k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/test"
	vpa_api_util "k8s.io/autoscaler/vertical-pod-autoscaler/pkg/utils/vpa"
)

func parseLabelSelector(selector string) labels.Selector {
	labelSelector, _ := metav1.ParseToLabelSelector(selector)
	parsedSelector, _ := metav1.LabelSelectorAsSelector(labelSelector)
	return parsedSelector
}

// newVPAIndexer returns an indexer, as used by the updater, holding the given VPAs.
func newVPAIndexer(t testing.TB, vpas ...*vpa_types.VerticalPodAutoscaler) cache.Indexer {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, vpa_api_util.VPAIndexers())
	for _, vpa := range vpas {
		if err := indexer.Add(vpa); err != nil {
			t.Fatal(err)
		}
	}
	return indexer
}

func TestRunOnce_Mode(t *testing.T) {
	tests := []struct {
		name                  string
		updateMode            vpa_types.UpdateMode
		shouldInPlaceFail     bool
		expectFetchCalls      bool
		expectedEvictionCount int
		expectedInPlacedCount int
		canEvict              bool
		canInPlaceUpdate      utils.InPlaceDecision
		isCPUBoostTest        bool
	}{
		{
			name:                  "with Auto mode",
			updateMode:            vpa_types.UpdateModeAuto, //nolint:staticcheck
			shouldInPlaceFail:     false,
			expectFetchCalls:      true,
			expectedEvictionCount: 5,
			expectedInPlacedCount: 0,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
		},
		{
			name:                  "with Initial mode",
			updateMode:            vpa_types.UpdateModeInitial,
			shouldInPlaceFail:     false,
			expectFetchCalls:      false,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
		},
		{
			name:                  "with Off mode",
			updateMode:            vpa_types.UpdateModeOff,
			shouldInPlaceFail:     false,
			expectFetchCalls:      false,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
		},
		{
			name:                  "with InPlaceOrRecreate mode expecting in-place updates",
			updateMode:            vpa_types.UpdateModeInPlaceOrRecreate,
			shouldInPlaceFail:     false,
			expectFetchCalls:      true,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 5,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
		},
		{
			name:                  "with InPlaceOrRecreate mode expecting fallback to evictions",
			updateMode:            vpa_types.UpdateModeInPlaceOrRecreate,
			shouldInPlaceFail:     false,
			expectFetchCalls:      true,
			expectedEvictionCount: 5,
			expectedInPlacedCount: 0,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceEvict,
		},
		{
			name:                  "with InPlaceOrRecreate mode expecting no evictions or in-place",
			updateMode:            vpa_types.UpdateModeInPlaceOrRecreate,
			shouldInPlaceFail:     false,
			expectFetchCalls:      true,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
			canEvict:              false,
			canInPlaceUpdate:      utils.InPlaceDeferred,
		},
		{
			name:                  "with InPlaceOrRecreate mode and failed in-place update",
			updateMode:            vpa_types.UpdateModeInPlaceOrRecreate,
			shouldInPlaceFail:     true,
			expectFetchCalls:      true,
			expectedEvictionCount: 5, // All pods should be evicted after in-place update fails
			expectedInPlacedCount: 5, // All pods attempt in-place update first
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
		},
		{
			name:                  "with InPlaceOrRecreate mode and unboost",
			updateMode:            vpa_types.UpdateModeInPlaceOrRecreate,
			shouldInPlaceFail:     false,
			expectFetchCalls:      true,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
			isCPUBoostTest:        true,
		},
		{
			name:                  "with Recreate mode and unboost",
			updateMode:            vpa_types.UpdateModeRecreate,
			shouldInPlaceFail:     false,
			expectFetchCalls:      true,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
			isCPUBoostTest:        true,
		},
		{
			name:                  "with Auto mode and unboost",
			updateMode:            vpa_types.UpdateModeAuto, //nolint:staticcheck
			shouldInPlaceFail:     false,
			expectFetchCalls:      true,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
			isCPUBoostTest:        true,
		},
		{
			name:                  "with InPlaceOrRecreate mode and unboost and In-place fails",
			updateMode:            vpa_types.UpdateModeInPlaceOrRecreate,
			shouldInPlaceFail:     true,
			expectFetchCalls:      true,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
			canEvict:              true,
			canInPlaceUpdate:      utils.InPlaceApproved,
			isCPUBoostTest:        true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testRunOnceBase(
				t,
				tc.updateMode,
				tc.shouldInPlaceFail,
				newFakeValidator(true),
				tc.expectFetchCalls,
				tc.expectedEvictionCount,
				tc.expectedInPlacedCount,
				tc.canInPlaceUpdate,
				tc.isCPUBoostTest,
			)
		})
	}
}

func TestRunOnce_Status(t *testing.T) {
	tests := []struct {
		name                  string
		statusValidator       status.Validator
		expectFetchCalls      bool
		expectedEvictionCount int
		expectedInPlacedCount int
	}{
		{
			name:                  "with valid status",
			statusValidator:       newFakeValidator(true),
			expectFetchCalls:      true,
			expectedEvictionCount: 5,
			expectedInPlacedCount: 0,
		},
		{
			name:                  "with invalid status",
			statusValidator:       newFakeValidator(false),
			expectFetchCalls:      false,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
		},
		{
			name:                  "with status check error",
			statusValidator:       newFakeValidatorErr(errors.New("failed to get status")),
			expectFetchCalls:      false,
			expectedEvictionCount: 0,
			expectedInPlacedCount: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testRunOnceBase(
				t,
				vpa_types.UpdateModeRecreate,
				false,
				tc.statusValidator,
				tc.expectFetchCalls,
				tc.expectedEvictionCount,
				tc.expectedInPlacedCount,
				utils.InPlaceApproved,
				false,
			)
		})
	}
}

func testRunOnceBase(
	t *testing.T,
	updateMode vpa_types.UpdateMode,
	shouldInPlaceFail bool,
	statusValidator status.Validator,
	expectFetchCalls bool,
	expectedEvictionCount int,
	expectedInPlacedCount int,
	canInPlaceUpdate utils.InPlaceDecision,
	isCPUBoostTest bool,
) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.CPUStartupBoost, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	replicas := int32(5)
	livePods := 5
	labels := map[string]string{"app": "testingApp"}
	selector := parseLabelSelector("app = testingApp")
	containerName := "container1"
	rc := corev1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rc",
			Namespace: "default",
		},
		Spec: corev1.ReplicationControllerSpec{
			Replicas: &replicas,
		},
	}
	pods := make([]*corev1.Pod, livePods)
	eviction := &test.PodsEvictionRestrictionMock{}
	inplace := &test.PodsInPlaceRestrictionMock{}

	vpaObj := test.VerticalPodAutoscaler().
		WithContainer(containerName).
		WithTarget("2", "200M").
		WithMinAllowed(containerName, "1", "100M").
		WithMaxAllowed(containerName, "3", "1G").
		WithTargetRef(&autoscalingv1.CrossVersionObjectReference{
			Kind:       rc.Kind,
			Name:       rc.Name,
			APIVersion: rc.APIVersion,
		}).
		Get()

	for i := range pods {
		pods[i] = test.Pod().WithName("test_"+strconv.Itoa(i)).
			AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).WithMemRequest(resource.MustParse("100M")).Get()).
			WithCreator(&rc.ObjectMeta, &rc.TypeMeta).
			Get()

		pods[i].Labels = labels
		if isCPUBoostTest {
			pods[i].Annotations = map[string]string{
				annotations.GetStartupCPUBoostAnnotationKey(containerName): "",
			}
			pods[i].Status.Conditions = []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			}
		}

		if !isCPUBoostTest {
			inplace.On("CanInPlaceUpdate", pods[i]).Return(canInPlaceUpdate)
			eviction.On("CanEvict", pods[i]).Return(true)
		} else {
			inplace.On("CanUnboost", pods[i], vpaObj).Return(isCPUBoostTest)
		}
		if shouldInPlaceFail {
			inplace.On("InPlaceUpdate", pods[i], nil).Return(errors.New("in-place update failed"))
		} else {
			inplace.On("InPlaceUpdate", pods[i], nil).Return(nil)
		}

		eviction.On("Evict", pods[i], nil).Return(nil)
	}

	factory := &restriction.FakePodsRestrictionFactory{
		Eviction: eviction,
		InPlace:  inplace,
	}

	podLister := &test.PodListerMock{}
	podLister.On("List").Return(pods, nil)

	vpaObj.Spec.UpdatePolicy = &vpa_types.PodUpdatePolicy{UpdateMode: &updateMode}
	if isCPUBoostTest {
		durationSeconds := int32(60)
		cpuStartupBoost := &vpa_types.GenericStartupBoost{
			Type:            vpa_types.FactorStartupBoostType,
			DurationSeconds: &durationSeconds,
		}
		vpaObj.Spec.StartupBoost = &vpa_types.StartupBoost{
			CPU: cpuStartupBoost,
		}
	}

	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)

	updater := &updater{
		vpaIndexer:                   newVPAIndexer(t, vpaObj),
		podLister:                    podLister,
		restrictionFactory:           factory,
		evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
		inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
		evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
		recommendationProcessor:      &test.FakeRecommendationProcessor{},
		selectorFetcher:              mockSelectorFetcher,
		controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
		useAdmissionControllerStatus: true,
		statusValidator:              statusValidator,
		priorityProcessor:            priority.NewProcessor(),
	}

	if expectFetchCalls {
		mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaObj)).Return(selector, nil)
	}
	updater.RunOnce(context.Background())
	eviction.AssertNumberOfCalls(t, "Evict", expectedEvictionCount)
	inplace.AssertNumberOfCalls(t, "InPlaceUpdate", expectedInPlacedCount)
}

// TestRunOnce_MultipleVPAs tests that if multiple VPAs target the same Pod and only one of them is 'active' (does NOT have `spec.updateMode: Off`):
// - the applied recommendation is always the one from the 'active' VPA
// - the 'active' VPA is always selected as the 'controlling' VPA for the Pod
func TestRunOnce_MultipleVPAs(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.InPlace, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	podLabels := map[string]string{"app": "test"}
	selector := parseLabelSelector("app = test")
	containerName := "test"
	replicas := int32(1)
	rs := appsv1.ReplicaSet{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicaSet",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rs",
			Namespace: "default",
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
		},
	}

	pod := test.Pod().WithName("test-pod").
		AddContainer(test.Container().WithName(containerName).
			WithCPURequest(resource.MustParse("1")).
			WithMemRequest(resource.MustParse("100M")).
			WithCPULimit(resource.MustParse("1")).
			WithMemLimit(resource.MustParse("100M")).Get()).
		WithLabels(podLabels).
		WithCreator(&rs.ObjectMeta, &rs.TypeMeta).
		Get()

	targetRef := &autoscalingv1.CrossVersionObjectReference{
		Kind:       rs.Kind,
		Name:       rs.Name,
		APIVersion: rs.APIVersion,
	}

	// If multiple VPAs match a Pod, a 'controlling' VPA is selected using VPA
	// timestamps.
	// The selector should be given a list that does NOT include any VPAs with
	// `updateMode: Off` and no startup boost, so the test here would catch if
	// that logic changed.
	now := time.Now()
	vpaInPlace := test.VerticalPodAutoscaler().
		WithName("vpa-in-place").
		WithNamespace("default").
		WithCreationTimestamp(now).
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeInPlace).
		WithTarget("3", "300M").
		WithTargetRef(targetRef).
		Get()

	vpaOff1 := test.VerticalPodAutoscaler().
		WithName("vpa-off-1").
		WithNamespace("default").
		WithCreationTimestamp(now.Add(-time.Hour)). // older than the InPlace VPA
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeOff).
		WithTarget("9", "900M").
		WithTargetRef(targetRef).
		Get()

	vpaOff2 := test.VerticalPodAutoscaler().
		WithName("vpa-off-2").
		WithNamespace("default").
		WithCreationTimestamp(now). // timestamp matches the InPlace VPA
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeOff).
		WithTarget("5", "500M").
		WithTargetRef(targetRef).
		Get()

	vpaOff3 := test.VerticalPodAutoscaler().
		WithName("vpa-off-3").
		WithNamespace("default").
		WithCreationTimestamp(now.Add(time.Hour)). // newer than the InPlace VPA
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeOff).
		WithTarget("7", "700M").
		WithTargetRef(targetRef).
		Get()

	kubeClient := fake.NewSimpleClientset(pod)

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	require.NoError(t, informerFactory.Apps().V1().ReplicaSets().Informer().GetStore().Add(&rs))

	limitRangeCalculator := limitrange.NewNoopLimitsCalculator()
	recommendationProvider := recommendation.NewProvider(limitRangeCalculator, vpa_api_util.NewCappingRecommendationProcessor(limitRangeCalculator))
	calculators := []patch.Calculator{inplace.NewResourceInPlaceUpdatesCalculator(recommendationProvider)}
	restrictionFactory := restriction.NewPodsRestrictionFactory(kubeClient, informerFactory, 0, 0, calculators, false)

	podLister := &test.PodListerMock{}
	podLister.On("List").Return([]*corev1.Pod{pod}, nil)

	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)
	mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaInPlace)).Return(selector, nil)

	u := &updater{
		vpaIndexer:                   newVPAIndexer(t, vpaInPlace, vpaOff1, vpaOff2, vpaOff3),
		podLister:                    podLister,
		restrictionFactory:           restrictionFactory,
		evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
		inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
		evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
		recommendationProcessor:      &test.FakeRecommendationProcessor{},
		selectorFetcher:              mockSelectorFetcher,
		controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
		useAdmissionControllerStatus: true,
		statusValidator:              newFakeValidator(true),
		priorityProcessor:            priority.NewProcessor(),
		eventRecorder:                record.NewFakeRecorder(10),
	}

	u.RunOnce(context.Background())

	updatedPod, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)

	// Test that the Pod got patched with the recommendation from the VPA with `updateMode: InPlace`.
	wantResources := vpaInPlace.Status.Recommendation.ContainerRecommendations[0].Target

	gotRequests := updatedPod.Spec.Containers[0].Resources.Requests
	test.AssertResourceListEqual(t, "requests", wantResources, gotRequests)

	gotLimits := updatedPod.Spec.Containers[0].Resources.Limits
	test.AssertResourceListEqual(t, "limits", wantResources, gotLimits)
}

func TestRunOnceNotingToProcess(t *testing.T) {
	eviction := &test.PodsEvictionRestrictionMock{}
	inplace := &test.PodsInPlaceRestrictionMock{}
	factory := &restriction.FakePodsRestrictionFactory{
		Eviction: eviction,
		InPlace:  inplace,
	}
	podLister := &test.PodListerMock{}

	updater := &updater{
		vpaIndexer:                   newVPAIndexer(t),
		podLister:                    podLister,
		restrictionFactory:           factory,
		evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
		inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
		evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
		recommendationProcessor:      &test.FakeRecommendationProcessor{},
		useAdmissionControllerStatus: true,
		statusValidator:              newFakeValidator(true),
	}
	updater.RunOnce(context.Background())
}

func TestGetRateLimiter(t *testing.T) {
	cases := []struct {
		rateLimit       float64
		rateLimitBurst  int
		expectedLimiter *rate.Limiter
	}{
		{0.0, 1, rate.NewLimiter(rate.Inf, 0)},
		{-1.0, 2, rate.NewLimiter(rate.Inf, 0)},
		{10.0, 3, rate.NewLimiter(rate.Limit(10), 3)},
	}
	for _, tc := range cases {
		limiter := getRateLimiter(tc.rateLimit, tc.rateLimitBurst)
		assert.Equal(t, tc.expectedLimiter.Burst(), limiter.Burst())
		assert.InDelta(t, float64(tc.expectedLimiter.Limit()), float64(limiter.Limit()), 1e-6)
	}
}

type fakeValidator struct {
	isValid bool
	err     error
}

func newFakeValidator(isValid bool) status.Validator {
	return &fakeValidator{isValid: isValid}
}

func newFakeValidatorErr(err error) status.Validator {
	return &fakeValidator{err: err}
}

func (f *fakeValidator) IsStatusValid(ctx context.Context, statusTimeout time.Duration) (bool, error) {
	return f.isValid, f.err
}

func TestRunOnceIgnoreNamespaceMatchingPods(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	replicas := int32(5)
	livePods := 5
	labels := map[string]string{"app": "testingApp"}
	selector := parseLabelSelector("app = testingApp")

	containerName := "container1"
	rc := corev1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rc",
			Namespace: "default",
		},
		Spec: corev1.ReplicationControllerSpec{
			Replicas: &replicas,
		},
	}
	pods := make([]*corev1.Pod, livePods)
	eviction := &test.PodsEvictionRestrictionMock{}
	inplace := &test.PodsInPlaceRestrictionMock{}
	for i := range pods {
		pods[i] = test.Pod().WithName("test_"+strconv.Itoa(i)).
			AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).WithMemRequest(resource.MustParse("100M")).Get()).
			WithCreator(&rc.ObjectMeta, &rc.TypeMeta).
			Get()

		pods[i].Labels = labels
		eviction.On("CanEvict", pods[i]).Return(true)
		eviction.On("Evict", pods[i], nil).Return(nil)
	}

	factory := &restriction.FakePodsRestrictionFactory{
		Eviction: eviction,
		InPlace:  inplace,
	}

	podLister := &test.PodListerMock{}
	podLister.On("List").Return(pods, nil)
	targetRef := &autoscalingv1.CrossVersionObjectReference{
		Kind:       rc.Kind,
		Name:       rc.Name,
		APIVersion: rc.APIVersion,
	}

	vpaObj := test.VerticalPodAutoscaler().
		WithNamespace("default").
		WithContainer(containerName).
		WithTarget("2", "200M").
		WithMinAllowed(containerName, "1", "100M").
		WithMaxAllowed(containerName, "3", "1G").
		WithTargetRef(targetRef).
		Get()

	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)
	mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaObj)).Return(selector, nil)

	updater := &updater{
		vpaIndexer:                   newVPAIndexer(t, vpaObj),
		podLister:                    podLister,
		restrictionFactory:           factory,
		evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
		inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
		evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
		recommendationProcessor:      &test.FakeRecommendationProcessor{},
		selectorFetcher:              mockSelectorFetcher,
		controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
		useAdmissionControllerStatus: true,
		priorityProcessor:            priority.NewProcessor(),
		ignoredNamespaces:            []string{"not-default"},
		statusValidator:              newFakeValidator(true),
	}

	updater.RunOnce(context.Background())
	eviction.AssertNumberOfCalls(t, "Evict", 5)
}

func TestRunOnceIgnoreNamespaceMatching(t *testing.T) {
	eviction := &test.PodsEvictionRestrictionMock{}
	vpaObj := test.VerticalPodAutoscaler().
		WithNamespace("default").
		WithContainer("container").Get()

	updater := &updater{
		vpaIndexer:        newVPAIndexer(t, vpaObj),
		ignoredNamespaces: []string{"default"},
	}

	updater.RunOnce(context.Background())
	eviction.AssertNumberOfCalls(t, "Evict", 0)
	eviction.AssertNumberOfCalls(t, "InPlaceUpdate", 0)
}

func TestNewEventRecorder(t *testing.T) {
	fakeClient := fake.NewClientset()
	er := newEventRecorder(fakeClient)

	maxRetries := 5
	retryDelay := 100 * time.Millisecond
	contextTimeout := 5 * time.Second

	testCases := []struct {
		reason  string
		object  runtime.Object
		message string
	}{
		{
			reason:  "EvictedPod",
			object:  &corev1.Pod{},
			message: "Evicted pod",
		},
		{
			reason:  "EvictedPod",
			object:  &vpa_types.VerticalPodAutoscaler{},
			message: "Evicted pod",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.reason, func(t *testing.T) {
			er.Event(tc.object, corev1.EventTypeNormal, tc.reason, tc.message)

			var events *corev1.EventList
			var err error
			// Add delay for fake client to catch up due to be being asynchronous
			for range maxRetries {
				ctx, cancel := context.WithTimeout(context.Background(), contextTimeout)
				defer cancel()
				events, err = fakeClient.CoreV1().Events("default").List(ctx, metav1.ListOptions{})
				if err == nil && len(events.Items) > 0 {
					break
				}
				time.Sleep(retryDelay)
			}

			assert.NoError(t, err, "should be able to list events")
			assert.Equal(t, 1, len(events.Items), "should have exactly 1 event")

			event := events.Items[0]
			assert.Equal(t, tc.reason, event.Reason)
			assert.Equal(t, tc.message, event.Message)
			assert.Equal(t, corev1.EventTypeNormal, event.Type)
			assert.Equal(t, "vpa-updater", event.Source.Component)
		})
	}
}

func TestLogDeprecationWarnings(t *testing.T) {
	tests := []struct {
		name             string
		updateMode       *vpa_types.UpdateMode
		updatePolicy     *vpa_types.PodUpdatePolicy
		shouldLogWarning bool
	}{
		{
			name:             "Auto mode should trigger deprecation warning logic",
			updateMode:       &[]vpa_types.UpdateMode{vpa_types.UpdateModeAuto}[0], //nolint:staticcheck
			shouldLogWarning: true,
		},
		{
			name:             "Recreate mode should not trigger warning logic",
			updateMode:       &[]vpa_types.UpdateMode{vpa_types.UpdateModeRecreate}[0],
			shouldLogWarning: false,
		},
		{
			name:             "Initial mode should not trigger warning logic",
			updateMode:       &[]vpa_types.UpdateMode{vpa_types.UpdateModeInitial}[0],
			shouldLogWarning: false,
		},
		{
			name:             "nil update policy should not trigger warning logic",
			updatePolicy:     nil,
			shouldLogWarning: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var updatePolicy *vpa_types.PodUpdatePolicy
			if tc.updatePolicy != nil {
				updatePolicy = tc.updatePolicy
			} else if tc.updateMode != nil {
				updatePolicy = &vpa_types.PodUpdatePolicy{
					UpdateMode: tc.updateMode,
				}
			}

			vpa := &vpa_types.VerticalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vpa",
					Namespace: "default",
				},
				Spec: vpa_types.VerticalPodAutoscalerSpec{
					UpdatePolicy: updatePolicy,
				},
			}

			shouldLogWarning := vpa.Spec.UpdatePolicy != nil &&
				vpa.Spec.UpdatePolicy.UpdateMode != nil &&
				*vpa.Spec.UpdatePolicy.UpdateMode == vpa_types.UpdateModeAuto //nolint:staticcheck

			assert.Equal(t, tc.shouldLogWarning, shouldLogWarning,
				"Expected shouldLogWarning=%v for test case %s", tc.shouldLogWarning, tc.name)

			// Call the function to ensure it doesn't panic
			logDeprecationWarnings(vpa)
		})
	}
}

func TestInfeasibleAttempts(t *testing.T) {
	containerName := "container1"
	pod1 := test.Pod().WithName("pod1").WithUID("pod1").AddContainer(test.Container().WithName(containerName).Get()).Get()
	pod2 := test.Pod().WithName("pod2").WithUID("pod2").AddContainer(test.Container().WithName(containerName).Get()).Get()

	vpa := test.VerticalPodAutoscaler().WithContainer(containerName).Get()

	// Initialize updater with required fields for these functions
	u := &updater{
		recommendationProcessor: &test.FakeRecommendationProcessor{},
		infeasibleAttempts:      make(map[types.UID]*vpa_types.RecommendedPodResources),
	}

	t.Run("recordInfeasibleAttempt stores recommendation", func(t *testing.T) {
		u.recordInfeasibleAttempt(pod1, vpa)
		u.recordInfeasibleAttempt(pod2, vpa)
		assert.Len(t, u.infeasibleAttempts, 2)
		assert.Contains(t, u.infeasibleAttempts, pod1.UID)
		assert.Contains(t, u.infeasibleAttempts, pod2.UID)
	})

	t.Run("cleanupStaleInfeasibleAttempts removes old pods", func(t *testing.T) {
		// Only pod1 is "live" now
		livePodUIDs := set.New(pod1.UID)
		u.cleanupStaleInfeasibleAttempts(livePodUIDs)
		// pod1 should stay, pod2 should be deleted
		assert.Len(t, u.infeasibleAttempts, 1)
		assert.Contains(t, u.infeasibleAttempts, pod1.UID)
		assert.NotContains(t, u.infeasibleAttempts, pod2.UID)
	})

	t.Run("cleanupStaleInfeasibleAttempts clears all if none live", func(t *testing.T) {
		u.cleanupStaleInfeasibleAttempts(set.New[types.UID]())
		assert.Empty(t, u.infeasibleAttempts)
	})
}

func TestRunOnce_AutoUnboostThenEvict(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.CPUStartupBoost, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	replicas := int32(5)
	livePods := 5
	labels := map[string]string{"app": "testingApp"}
	selector := parseLabelSelector("app = testingApp")
	containerName := "container1"
	rc := corev1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "rc", Namespace: "default"},
		Spec:       corev1.ReplicationControllerSpec{Replicas: &replicas},
	}
	pods := make([]*corev1.Pod, livePods)
	vpaObj := test.VerticalPodAutoscaler().
		WithContainer(containerName).
		WithTarget("2", "200M").
		WithMinAllowed(containerName, "1", "100M").
		WithMaxAllowed(containerName, "3", "1G").
		WithTargetRef(&autoscalingv1.CrossVersionObjectReference{Kind: rc.Kind, Name: rc.Name, APIVersion: rc.APIVersion}).
		WithCPUStartupBoost(vpa_types.FactorStartupBoostType, nil, nil, 60).
		Get()

	for i := range pods {
		pods[i] = test.Pod().WithName("test_"+strconv.Itoa(i)).
			AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).WithMemRequest(resource.MustParse("100M")).Get()).
			WithCreator(&rc.ObjectMeta, &rc.TypeMeta).
			Get()
		pods[i].Labels = labels
	}

	eviction := &test.PodsEvictionRestrictionMock{}
	inplace := &test.PodsInPlaceRestrictionMock{}
	factory := &restriction.FakePodsRestrictionFactory{Eviction: eviction, InPlace: inplace}
	podLister := &test.PodListerMock{}
	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)

	updater := &updater{
		vpaIndexer:                   newVPAIndexer(t, vpaObj),
		podLister:                    podLister,
		restrictionFactory:           factory,
		evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
		inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
		evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
		recommendationProcessor:      &test.FakeRecommendationProcessor{},
		selectorFetcher:              mockSelectorFetcher,
		controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
		useAdmissionControllerStatus: true,
		statusValidator:              newFakeValidator(true),
		priorityProcessor:            priority.NewProcessor(),
	}

	// Cycle 1: Pods have boost annotations — RunOnce skips them (handled by queue worker)
	for i := range pods {
		pods[i].Annotations = map[string]string{annotations.GetStartupCPUBoostAnnotationKey(containerName): ""}
		pods[i].Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			},
		}
	}
	podLister.On("List").Return(pods, nil).Once()
	mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaObj)).Return(selector, nil)

	updater.RunOnce(context.Background())
	inplace.AssertNumberOfCalls(t, "InPlaceUpdate", 0)
	eviction.AssertNumberOfCalls(t, "Evict", 0)

	// Cycle 2: Boost annotations removed — pods go through normal eviction path
	for i := range pods {
		pods[i].Annotations = nil
		eviction.On("CanEvict", pods[i]).Return(true)
		eviction.On("Evict", pods[i], nil).Return(nil)
	}
	podLister.On("List").Return(pods, nil).Once()
	mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaObj)).Return(selector, nil)

	updater.RunOnce(context.Background())
	inplace.AssertNumberOfCalls(t, "InPlaceUpdate", 0)
	eviction.AssertNumberOfCalls(t, "Evict", 5)
}

func TestRunOnce_AutoUnboostThenInPlace(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.CPUStartupBoost, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	replicas := int32(5)
	livePods := 5
	labels := map[string]string{"app": "testingApp"}
	selector := parseLabelSelector("app = testingApp")
	containerName := "container1"
	rc := corev1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "rc", Namespace: "default"},
		Spec:       corev1.ReplicationControllerSpec{Replicas: &replicas},
	}
	pods := make([]*corev1.Pod, livePods)
	vpaObj := test.VerticalPodAutoscaler().
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeInPlaceOrRecreate).
		WithTarget("2", "200M").
		WithMinAllowed(containerName, "1", "100M").
		WithMaxAllowed(containerName, "3", "1G").
		WithTargetRef(&autoscalingv1.CrossVersionObjectReference{Kind: rc.Kind, Name: rc.Name, APIVersion: rc.APIVersion}).
		WithCPUStartupBoost(vpa_types.FactorStartupBoostType, nil, nil, 60).
		Get()

	for i := range pods {
		pods[i] = test.Pod().WithName("test_"+strconv.Itoa(i)).
			AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).WithMemRequest(resource.MustParse("100M")).Get()).
			WithCreator(&rc.ObjectMeta, &rc.TypeMeta).
			Get()
		pods[i].Labels = labels
	}

	eviction := &test.PodsEvictionRestrictionMock{}
	inplace := &test.PodsInPlaceRestrictionMock{}
	factory := &restriction.FakePodsRestrictionFactory{Eviction: eviction, InPlace: inplace}
	podLister := &test.PodListerMock{}
	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)

	updater := &updater{
		vpaIndexer:                   newVPAIndexer(t, vpaObj),
		podLister:                    podLister,
		restrictionFactory:           factory,
		evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
		inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
		evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
		recommendationProcessor:      &test.FakeRecommendationProcessor{},
		selectorFetcher:              mockSelectorFetcher,
		controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
		useAdmissionControllerStatus: true,
		statusValidator:              newFakeValidator(true),
		priorityProcessor:            priority.NewProcessor(),
	}

	// Cycle 1: Pods have boost annotations — RunOnce skips them (handled by queue worker)
	for i := range pods {
		pods[i].Annotations = map[string]string{annotations.GetStartupCPUBoostAnnotationKey(containerName): ""}
		pods[i].Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			},
		}
	}
	podLister.On("List").Return(pods, nil).Once()
	mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaObj)).Return(selector, nil)

	updater.RunOnce(context.Background())
	inplace.AssertNumberOfCalls(t, "InPlaceUpdate", 0)
	eviction.AssertNumberOfCalls(t, "Evict", 0)

	// Cycle 2: Boost annotations removed — pods go through normal in-place update path
	for i := range pods {
		pods[i].Annotations = nil
		inplace.On("CanInPlaceUpdate", pods[i]).Return(utils.InPlaceApproved)
		inplace.On("InPlaceUpdate", pods[i], nil).Return(nil)
	}
	podLister.On("List").Return(pods, nil).Once()
	mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaObj)).Return(selector, nil)

	updater.RunOnce(context.Background())
	inplace.AssertNumberOfCalls(t, "InPlaceUpdate", 5)
	eviction.AssertNumberOfCalls(t, "Evict", 0)
}

func TestIsInfeasibleError(t *testing.T) {
	makeStatusErr := func(reason metav1.StatusReason, causeType metav1.CauseType) error {
		return &apierrors.StatusError{
			ErrStatus: metav1.Status{
				Reason: reason,
				Details: &metav1.StatusDetails{
					Causes: []metav1.StatusCause{
						{Type: causeType},
					},
				},
			},
		}
	}

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "NodeCapacity cause returns true",
			err:  makeStatusErr(metav1.StatusReasonForbidden, "NodeCapacity"),
			want: true,
		},
		{
			name: "Forbidden with unknown cause returns false",
			err:  makeStatusErr(metav1.StatusReasonForbidden, "SomethingElse"),
			want: false,
		},
		{
			name: "Non-Forbidden status error returns false",
			err:  makeStatusErr(metav1.StatusReasonNotFound, "NodeCapacity"),
			want: false,
		},
		{
			name: "Plain error returns false",
			err:  errors.New("plain error"),
			want: false,
		},
		{
			name: "Nil error returns false",
			err:  nil,
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isInfeasibleError(tc.err))
		})
	}
}

type podNamespaceListerStub struct {
	pods []*corev1.Pod
}

func (s *podNamespaceListerStub) List(_ labels.Selector) ([]*corev1.Pod, error) {
	return s.pods, nil
}

func (s *podNamespaceListerStub) Get(name string) (*corev1.Pod, error) {
	for _, p := range s.pods {
		if p.Name == name {
			return p, nil
		}
	}
	return nil, apierrors.NewNotFound(corev1.Resource("pods"), name)
}

func TestProcessNextBoostItem_RetryExhaustion_ThenResync(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.CPUStartupBoost, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	containerName := "container1"
	replicas := int32(1)
	rc := corev1.ReplicationController{
		TypeMeta:   metav1.TypeMeta{Kind: "ReplicationController", APIVersion: "apps/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "rc", Namespace: "default"},
		Spec:       corev1.ReplicationControllerSpec{Replicas: &replicas},
	}

	pod := test.Pod().WithName("boost-pod").
		AddContainer(test.Container().WithName(containerName).WithCPURequest(resource.MustParse("1")).WithMemRequest(resource.MustParse("100M")).Get()).
		WithCreator(&rc.ObjectMeta, &rc.TypeMeta).
		WithLabels(map[string]string{"app": "testingApp"}).
		WithAnnotations(map[string]string{annotations.GetStartupCPUBoostAnnotationKey(containerName): ""}).
		WithPodConditions([]corev1.PodCondition{{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-2 * time.Minute)),
		}}).
		Get()

	selector := parseLabelSelector("app = testingApp")
	vpaObj := test.VerticalPodAutoscaler().
		WithContainer(containerName).
		WithTarget("2", "200M").
		WithMinAllowed(containerName, "1", "100M").
		WithMaxAllowed(containerName, "3", "1G").
		WithTargetRef(&autoscalingv1.CrossVersionObjectReference{Kind: rc.Kind, Name: rc.Name, APIVersion: rc.APIVersion}).
		WithCPUStartupBoost(vpa_types.FactorStartupBoostType, nil, nil, 60).
		Get()

	inplace := &test.PodsInPlaceRestrictionMock{}
	factory := &restriction.FakePodsRestrictionFactory{InPlace: inplace}

	podNSLister := &podNamespaceListerStub{pods: []*corev1.Pod{pod}}
	podLister := &test.PodListerMock{}
	podLister.On("Pods", "default").Return(podNSLister)

	// The VPA is added to the indexer only once retries are exhausted.
	vpaIndexer := newVPAIndexer(t)

	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)
	mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaObj)).Return(selector, nil)

	queue := workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.NewTypedItemExponentialFailureRateLimiter[string](0, 0),
		workqueue.TypedRateLimitingQueueConfig[string]{Name: "test-boost"},
	)
	defer queue.ShutDown()

	u := &updater{
		vpaIndexer:           vpaIndexer,
		podLister:            podLister,
		restrictionFactory:   factory,
		inPlaceRateLimiter:   rate.NewLimiter(rate.Inf, 0),
		selectorFetcher:      mockSelectorFetcher,
		controllerFetcher:    controllerfetcher.FakeControllerFetcher{},
		cpuStartupBoostQueue: queue,
	}

	ctx := context.Background()
	queue.Add("default/boost-pod")

	// Phase 1: exhaust VPA-lookup retries.
	// Calls 1-6 find no VPA and re-enqueue (NumRequeues 0→5, all ≤ maxVPALookupRetries).
	// Call 7: NumRequeues=6 > 5, item is forgotten.
	for i := range maxVPALookupRetries + 2 {
		assert.True(t, u.processNextBoostItem(ctx), "iteration %d", i)
	}
	assert.Equal(t, 0, queue.Len(), "queue should be empty after retry exhaustion")

	// Phase 2: resync re-enqueues the pod via PodUpdateHandler.
	u.PodUpdateHandler(nil, pod)
	assert.Equal(t, 1, queue.Len(), "queue should have 1 item after resync")

	// Phase 3: VPA is now resolvable. Boost expired, unboost succeeds.
	assert.NoError(t, vpaIndexer.Add(vpaObj))
	inplace.On("CanUnboost", pod, vpaObj).Return(true)
	inplace.On("InPlaceUpdate", pod, nil).Return(nil)

	assert.True(t, u.processNextBoostItem(ctx))
	inplace.AssertNumberOfCalls(t, "InPlaceUpdate", 1)
	assert.Equal(t, 0, queue.Len(), "queue should be empty after successful unboost")
}

func evictedPodNames(actions []clienttesting.Action) []string {
	var names []string
	for _, action := range actions {
		if action.GetVerb() == "create" && action.GetSubresource() == "eviction" {
			createAction, ok := action.(clienttesting.CreateAction)
			if !ok {
				continue
			}
			if eviction, ok := createAction.GetObject().(*policyv1.Eviction); ok {
				names = append(names, eviction.Name)
			}
		}
	}
	return names
}

func TestRunOnce_NodeSelectorPartitioningAndFallback(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.InPlace, true)
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.VPANodeSelector, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	podLabels := map[string]string{"app": "agent"}
	selector := parseLabelSelector("app = agent")
	containerName := "agent"
	ds := appsv1.DaemonSet{
		TypeMeta:   metav1.TypeMeta{Kind: "DaemonSet", APIVersion: "apps/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "agent-ds", Namespace: "default"},
		Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 3},
	}
	targetRef := &autoscalingv1.CrossVersionObjectReference{
		Kind:       ds.Kind,
		Name:       ds.Name,
		APIVersion: ds.APIVersion,
	}

	nodes := []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "node-gpu", Labels: map[string]string{"pool": "gpu"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "node-mem", Labels: map[string]string{"pool": "highmem"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "node-cpu", Labels: map[string]string{"pool": "cpu"}}},
	}

	makePodOnNode := func(name, nodeName string) *corev1.Pod {
		return test.Pod().WithName(name).
			WithNodeName(nodeName).
			AddContainer(test.Container().WithName(containerName).
				WithCPURequest(resource.MustParse("500m")).
				WithMemRequest(resource.MustParse("100M")).
				WithCPULimit(resource.MustParse("500m")).
				WithMemLimit(resource.MustParse("100M")).Get()).
			WithLabels(podLabels).
			WithCreator(&ds.ObjectMeta, &ds.TypeMeta).
			Get()
	}

	now := time.Now()
	vpaGPU := test.VerticalPodAutoscaler().
		WithName("vpa-gpu").
		WithNamespace("default").
		WithCreationTimestamp(now).
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeInPlace).
		WithMinReplicas(ptr.To(int32(1))).
		WithTarget("4", "400M").
		WithTargetRef(targetRef).
		WithNodeSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": "gpu"}}).
		Get()
	vpaMem := test.VerticalPodAutoscaler().
		WithName("vpa-mem").
		WithNamespace("default").
		WithCreationTimestamp(now).
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeInPlace).
		WithMinReplicas(ptr.To(int32(1))).
		WithTarget("2", "800M").
		WithTargetRef(targetRef).
		WithNodeSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": "highmem"}}).
		Get()
	vpaFallback := test.VerticalPodAutoscaler().
		WithName("vpa-fallback").
		WithNamespace("default").
		WithCreationTimestamp(now.Add(-time.Hour)). // older than selector VPAs, yet selector VPAs still win!
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeInPlace).
		WithMinReplicas(ptr.To(int32(1))).
		WithTarget("1", "200M").
		WithTargetRef(targetRef).
		Get()

	t.Run("pods match their node selector VPA and uncovered node falls back to plain VPA", func(t *testing.T) {
		podGPU := makePodOnNode("pod-gpu", "node-gpu")
		podMem := makePodOnNode("pod-mem", "node-mem")
		podCPU := makePodOnNode("pod-cpu", "node-cpu")
		kubeClient := fake.NewSimpleClientset(podGPU, podMem, podCPU)
		informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
		require.NoError(t, informerFactory.Apps().V1().DaemonSets().Informer().GetStore().Add(&ds))
		nodeStore := informerFactory.Core().V1().Nodes().Informer().GetStore()
		for _, n := range nodes {
			require.NoError(t, nodeStore.Add(n))
		}

		limitRangeCalc := limitrange.NewNoopLimitsCalculator()
		recProvider := recommendation.NewProvider(limitRangeCalc, vpa_api_util.NewCappingRecommendationProcessor(limitRangeCalc))
		calculators := []patch.Calculator{inplace.NewResourceInPlaceUpdatesCalculator(recProvider)}
		restrictionFactory := restriction.NewPodsRestrictionFactory(kubeClient, informerFactory, 2, 0.5, calculators, false)

		podLister := &test.PodListerMock{}
		podLister.On("List").Return([]*corev1.Pod{podGPU, podMem, podCPU}, nil)

		mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)
		mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaGPU)).Return(selector, nil)
		mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaMem)).Return(selector, nil)
		mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaFallback)).Return(selector, nil)

		u := &updater{
			vpaIndexer:                   newVPAIndexer(t, vpaGPU, vpaMem, vpaFallback),
			podLister:                    podLister,
			nodeLister:                   informerFactory.Core().V1().Nodes().Lister(),
			restrictionFactory:           restrictionFactory,
			evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
			inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
			evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
			recommendationProcessor:      &test.FakeRecommendationProcessor{},
			selectorFetcher:              mockSelectorFetcher,
			controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
			useAdmissionControllerStatus: true,
			statusValidator:              newFakeValidator(true),
			priorityProcessor:            priority.NewProcessor(),
			eventRecorder:                record.NewFakeRecorder(10),
		}

		u.RunOnce(context.Background())

		gotGPU, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "pod-gpu", metav1.GetOptions{})
		require.NoError(t, err)
		test.AssertResourceListEqual(t, "pod-gpu requests", vpaGPU.Status.Recommendation.ContainerRecommendations[0].Target, gotGPU.Spec.Containers[0].Resources.Requests)

		gotMem, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "pod-mem", metav1.GetOptions{})
		require.NoError(t, err)
		test.AssertResourceListEqual(t, "pod-mem requests", vpaMem.Status.Recommendation.ContainerRecommendations[0].Target, gotMem.Spec.Containers[0].Resources.Requests)

		gotCPU, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "pod-cpu", metav1.GetOptions{})
		require.NoError(t, err)
		test.AssertResourceListEqual(t, "pod-cpu requests", vpaFallback.Status.Recommendation.ContainerRecommendations[0].Target, gotCPU.Spec.Containers[0].Resources.Requests)
	})

	t.Run("pod on uncovered node is skipped when no fallback VPA exists", func(t *testing.T) {
		podGPU := makePodOnNode("pod-gpu", "node-gpu")
		podCPU := makePodOnNode("pod-cpu", "node-cpu")
		origCPUReqs := podCPU.Spec.Containers[0].Resources.Requests.DeepCopy()

		kubeClient := fake.NewSimpleClientset(podGPU, podCPU)
		informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
		require.NoError(t, informerFactory.Apps().V1().DaemonSets().Informer().GetStore().Add(&ds))
		nodeStore := informerFactory.Core().V1().Nodes().Informer().GetStore()
		for _, n := range nodes {
			require.NoError(t, nodeStore.Add(n))
		}

		limitRangeCalc := limitrange.NewNoopLimitsCalculator()
		recProvider := recommendation.NewProvider(limitRangeCalc, vpa_api_util.NewCappingRecommendationProcessor(limitRangeCalc))
		calculators := []patch.Calculator{inplace.NewResourceInPlaceUpdatesCalculator(recProvider)}
		restrictionFactory := restriction.NewPodsRestrictionFactory(kubeClient, informerFactory, 2, 0.5, calculators, false)

		podLister := &test.PodListerMock{}
		podLister.On("List").Return([]*corev1.Pod{podGPU, podCPU}, nil)

		mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)
		mockSelectorFetcher.EXPECT().Fetch(gomock.Eq(vpaGPU)).Return(selector, nil)

		u := &updater{
			vpaIndexer:                   newVPAIndexer(t, vpaGPU),
			podLister:                    podLister,
			nodeLister:                   informerFactory.Core().V1().Nodes().Lister(),
			restrictionFactory:           restrictionFactory,
			evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
			inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
			evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
			recommendationProcessor:      &test.FakeRecommendationProcessor{},
			selectorFetcher:              mockSelectorFetcher,
			controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
			useAdmissionControllerStatus: true,
			statusValidator:              newFakeValidator(true),
			priorityProcessor:            priority.NewProcessor(),
			eventRecorder:                record.NewFakeRecorder(10),
		}

		u.RunOnce(context.Background())

		gotGPU, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "pod-gpu", metav1.GetOptions{})
		require.NoError(t, err)
		test.AssertResourceListEqual(t, "pod-gpu requests", vpaGPU.Status.Recommendation.ContainerRecommendations[0].Target, gotGPU.Spec.Containers[0].Resources.Requests)

		gotCPU, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "pod-cpu", metav1.GetOptions{})
		require.NoError(t, err)
		test.AssertResourceListEqual(t, "pod-cpu unchanged requests", origCPUReqs, gotCPU.Spec.Containers[0].Resources.Requests)
	})
}

func TestRunOnce_NodeSelectorDisruptionBudgetOffInitialAndTerminatingPods(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.VPANodeSelector, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	podLabels := map[string]string{"app": "agent"}
	selector := parseLabelSelector("app = agent")
	containerName := "agent"
	// 10-node DaemonSet: DesiredNumberScheduled=10 would block any <=5 pod partition without scoped configured!
	ds := appsv1.DaemonSet{
		TypeMeta:   metav1.TypeMeta{Kind: "DaemonSet", APIVersion: "apps/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "agent-ds", Namespace: "default"},
		Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 10},
	}
	targetRef := &autoscalingv1.CrossVersionObjectReference{
		Kind:       ds.Kind,
		Name:       ds.Name,
		APIVersion: ds.APIVersion,
	}

	// Nodes:
	// - gpu: 3 nodes (controlled by vpa-gpu, Recreate)
	// - off: 2 nodes (controlled by vpa-off, Off -> must NOT be evicted by vpa-fallback)
	// - initial: 1 node (controlled by vpa-initial, Initial -> must NOT be evicted by vpa-fallback)
	// - cpu: 4 nodes (controlled by vpa-fallback, Recreate)
	var nodes []*corev1.Node
	var pods []*corev1.Pod
	addNodeAndPod := func(podName, nodeName, pool string) *corev1.Pod {
		nodes = append(nodes, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{"pool": pool}},
		})
		p := test.Pod().WithName(podName).
			WithUID(types.UID(podName)).
			WithNodeName(nodeName).
			AddContainer(test.Container().WithName(containerName).
				WithCPURequest(resource.MustParse("100m")).
				WithMemRequest(resource.MustParse("100M")).Get()).
			WithLabels(podLabels).
			WithCreator(&ds.ObjectMeta, &ds.TypeMeta).
			Get()
		pods = append(pods, p)
		return p
	}

	gpuPod1 := addNodeAndPod("pod-gpu-1", "node-gpu-1", "gpu")
	gpuPod2 := addNodeAndPod("pod-gpu-2", "node-gpu-2", "gpu")
	gpuPod3 := addNodeAndPod("pod-gpu-3", "node-gpu-3", "gpu")
	addNodeAndPod("pod-off-1", "node-off-1", "off")
	addNodeAndPod("pod-off-2", "node-off-2", "off")
	addNodeAndPod("pod-init-1", "node-init-1", "initial")
	addNodeAndPod("pod-cpu-1", "node-cpu-1", "cpu")
	addNodeAndPod("pod-cpu-2", "node-cpu-2", "cpu")
	addNodeAndPod("pod-cpu-3", "node-cpu-3", "cpu")
	addNodeAndPod("pod-cpu-4", "node-cpu-4", "cpu")

	now := time.Now()
	vpaFallback := test.VerticalPodAutoscaler().
		WithName("vpa-fallback").
		WithNamespace("default").
		WithCreationTimestamp(now.Add(-2 * time.Hour)). // oldest VPA
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeRecreate).
		WithTarget("2", "500M").
		WithTargetRef(targetRef).
		Get()
	vpaGPU := test.VerticalPodAutoscaler().
		WithName("vpa-gpu").
		WithNamespace("default").
		WithCreationTimestamp(now).
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeRecreate).
		WithTarget("4", "1G").
		WithTargetRef(targetRef).
		WithNodeSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": "gpu"}}).
		Get()
	vpaOff := test.VerticalPodAutoscaler().
		WithName("vpa-off").
		WithNamespace("default").
		WithCreationTimestamp(now.Add(time.Hour)). // newer than fallback, still wins because HasSelector!
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeOff).
		WithTarget("8", "2G").
		WithTargetRef(targetRef).
		WithNodeSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": "off"}}).
		Get()
	vpaInitial := test.VerticalPodAutoscaler().
		WithName("vpa-initial").
		WithNamespace("default").
		WithCreationTimestamp(now.Add(time.Hour)).
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeInitial).
		WithTarget("6", "1500M").
		WithTargetRef(targetRef).
		WithNodeSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": "initial"}}).
		Get()

	var runtimeObjs []runtime.Object
	for _, p := range pods {
		runtimeObjs = append(runtimeObjs, p)
	}
	kubeClient := fake.NewSimpleClientset(runtimeObjs...)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	require.NoError(t, informerFactory.Apps().V1().DaemonSets().Informer().GetStore().Add(&ds))
	nodeStore := informerFactory.Core().V1().Nodes().Informer().GetStore()
	for _, n := range nodes {
		require.NoError(t, nodeStore.Add(n))
	}

	restrictionFactory := restriction.NewPodsRestrictionFactory(kubeClient, informerFactory, 2, 0.5, nil, false)
	podLister := &test.PodListerMock{}
	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)

	u := &updater{
		vpaIndexer:                   newVPAIndexer(t, vpaFallback, vpaGPU, vpaOff, vpaInitial),
		podLister:                    podLister,
		nodeLister:                   informerFactory.Core().V1().Nodes().Lister(),
		restrictionFactory:           restrictionFactory,
		evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
		inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
		evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
		recommendationProcessor:      &test.FakeRecommendationProcessor{},
		selectorFetcher:              mockSelectorFetcher,
		controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
		useAdmissionControllerStatus: true,
		statusValidator:              newFakeValidator(true),
		priorityProcessor:            priority.NewProcessor(),
		eventRecorder:                record.NewFakeRecorder(20),
	}

	// Cycle 1: all 10 pods live.
	// - vpa-gpu (3 pods, tolerance=0.5 -> budget 1): evicts 1 gpu pod.
	// - vpa-fallback (4 cpu pods, tolerance=0.5 -> budget 2): evicts 2 cpu pods.
	// - vpa-off and vpa-initial pods: 0 evictions.
	podLister.On("List").Return(pods, nil).Once()
	mockSelectorFetcher.EXPECT().Fetch(gomock.Any()).Return(selector, nil).Times(4)

	u.RunOnce(context.Background())

	evictedCycle1 := evictedPodNames(kubeClient.Actions())
	assert.Len(t, evictedCycle1, 3, "expected 1 gpu pod + 2 cpu pods evicted, got %v", evictedCycle1)
	gpuEvictedCount := 0
	cpuEvictedCount := 0
	for _, name := range evictedCycle1 {
		assert.NotContains(t, []string{"pod-off-1", "pod-off-2", "pod-init-1"}, name, "Off and Initial selector VPA pods must never be evicted by fallback VPA")
		switch name {
		case "pod-gpu-1", "pod-gpu-2", "pod-gpu-3":
			gpuEvictedCount++
		case "pod-cpu-1", "pod-cpu-2", "pod-cpu-3", "pod-cpu-4":
			cpuEvictedCount++
		}
	}
	assert.Equal(t, 1, gpuEvictedCount, "vpa-gpu should evict exactly 1 of its 3 pods")
	assert.Equal(t, 2, cpuEvictedCount, "vpa-fallback should evict exactly 2 of its 4 pods")

	// Cycle 2: mark the evicted gpu pod as terminating (DeletionTimestamp != nil), while the other 2 gpu pods remain live.
	// Because terminating pods are counted in configured (configured=3, live=2, tolerance=1 -> shouldBeAlive=2),
	// vpa-gpu MUST NOT evict a second gpu pod while the first is still terminating!
	kubeClient.ClearActions()
	nowMeta := metav1.NewTime(now)
	for _, p := range []*corev1.Pod{gpuPod1, gpuPod2, gpuPod3} {
		if slices.Contains(evictedCycle1, p.Name) {
			p.DeletionTimestamp = &nowMeta
		}
	}
	// Also mark the 2 evicted cpu pods as terminating so vpa-fallback also has configured=4, live=2 -> 0 more evictions.
	for _, p := range pods {
		if slices.Contains(evictedCycle1, p.Name) {
			p.DeletionTimestamp = &nowMeta
		}
	}

	podLister.On("List").Return(pods, nil).Once()
	mockSelectorFetcher.EXPECT().Fetch(gomock.Any()).Return(selector, nil).Times(4)

	u.RunOnce(context.Background())
	evictedCycle2 := evictedPodNames(kubeClient.Actions())
	assert.Empty(t, evictedCycle2, "no additional pods should be evicted while evicted pods are terminating")
}

func TestRunOnce_NodeMissingFromCacheSkippedUntilPresent(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.InPlace, true)
	featuregatetesting.SetFeatureGateDuringTest(t, features.MutableFeatureGate, features.VPANodeSelector, true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	podLabels := map[string]string{"app": "agent"}
	selector := parseLabelSelector("app = agent")
	containerName := "agent"
	ds := appsv1.DaemonSet{
		TypeMeta:   metav1.TypeMeta{Kind: "DaemonSet", APIVersion: "apps/v1"},
		ObjectMeta: metav1.ObjectMeta{Name: "agent-ds", Namespace: "default"},
		Status:     appsv1.DaemonSetStatus{DesiredNumberScheduled: 1},
	}
	targetRef := &autoscalingv1.CrossVersionObjectReference{
		Kind:       ds.Kind,
		Name:       ds.Name,
		APIVersion: ds.APIVersion,
	}

	pod := test.Pod().WithName("pod-missing-node").
		WithNodeName("node-delayed").
		AddContainer(test.Container().WithName(containerName).
			WithCPURequest(resource.MustParse("500m")).
			WithMemRequest(resource.MustParse("100M")).
			WithCPULimit(resource.MustParse("500m")).
			WithMemLimit(resource.MustParse("100M")).Get()).
		WithLabels(podLabels).
		WithCreator(&ds.ObjectMeta, &ds.TypeMeta).
		Get()
	origRequests := pod.Spec.Containers[0].Resources.Requests.DeepCopy()

	vpaGPU := test.VerticalPodAutoscaler().
		WithName("vpa-gpu").
		WithNamespace("default").
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeInPlace).
		WithMinReplicas(ptr.To(int32(1))).
		WithTarget("4", "400M").
		WithTargetRef(targetRef).
		WithNodeSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": "gpu"}}).
		Get()
	vpaFallback := test.VerticalPodAutoscaler().
		WithName("vpa-fallback").
		WithNamespace("default").
		WithContainer(containerName).
		WithUpdateMode(vpa_types.UpdateModeInPlace).
		WithMinReplicas(ptr.To(int32(1))).
		WithTarget("1", "200M").
		WithTargetRef(targetRef).
		Get()

	kubeClient := fake.NewSimpleClientset(pod)
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	require.NoError(t, informerFactory.Apps().V1().DaemonSets().Informer().GetStore().Add(&ds))
	nodeStore := informerFactory.Core().V1().Nodes().Informer().GetStore()

	limitRangeCalc := limitrange.NewNoopLimitsCalculator()
	recProvider := recommendation.NewProvider(limitRangeCalc, vpa_api_util.NewCappingRecommendationProcessor(limitRangeCalc))
	calculators := []patch.Calculator{inplace.NewResourceInPlaceUpdatesCalculator(recProvider)}
	restrictionFactory := restriction.NewPodsRestrictionFactory(kubeClient, informerFactory, 1, 0.5, calculators, false)

	podLister := &test.PodListerMock{}
	mockSelectorFetcher := target_mock.NewMockVpaTargetSelectorFetcher(ctrl)

	u := &updater{
		vpaIndexer:                   newVPAIndexer(t, vpaGPU, vpaFallback),
		podLister:                    podLister,
		nodeLister:                   informerFactory.Core().V1().Nodes().Lister(),
		restrictionFactory:           restrictionFactory,
		evictionRateLimiter:          rate.NewLimiter(rate.Inf, 0),
		inPlaceRateLimiter:           rate.NewLimiter(rate.Inf, 0),
		evictionAdmission:            priority.NewDefaultPodEvictionAdmission(),
		recommendationProcessor:      &test.FakeRecommendationProcessor{},
		selectorFetcher:              mockSelectorFetcher,
		controllerFetcher:            controllerfetcher.FakeControllerFetcher{},
		useAdmissionControllerStatus: true,
		statusValidator:              newFakeValidator(true),
		priorityProcessor:            priority.NewProcessor(),
		eventRecorder:                record.NewFakeRecorder(10),
	}

	// Cycle 1: node-delayed is missing from nodeStore -> pod is skipped (NOT given fallback VPA!).
	podLister.On("List").Return([]*corev1.Pod{pod}, nil).Once()
	mockSelectorFetcher.EXPECT().Fetch(gomock.Any()).Return(selector, nil).Times(2)
	u.RunOnce(context.Background())

	gotBefore, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	test.AssertResourceListEqual(t, "requests unchanged while node missing from cache", origRequests, gotBefore.Spec.Containers[0].Resources.Requests)

	// Cycle 2: node-delayed appears in nodeStore with pool=gpu -> pod is updated with vpa-gpu!
	require.NoError(t, nodeStore.Add(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-delayed", Labels: map[string]string{"pool": "gpu"}},
	}))
	podLister.On("List").Return([]*corev1.Pod{pod}, nil).Once()
	mockSelectorFetcher.EXPECT().Fetch(gomock.Any()).Return(selector, nil).Times(2)
	u.RunOnce(context.Background())

	gotAfter, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), pod.Name, metav1.GetOptions{})
	require.NoError(t, err)
	test.AssertResourceListEqual(t, "requests updated once node appears in cache", vpaGPU.Status.Recommendation.ContainerRecommendations[0].Target, gotAfter.Spec.Containers[0].Resources.Requests)
}
