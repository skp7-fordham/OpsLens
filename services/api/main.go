package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

type PodInfo struct {
	Name     string `json:"name"`
	Status   string `json:"status"`
	Ready    bool   `json:"ready"`
	Restarts int32  `json:"restarts"`
}

type EventInfo struct {
	InvolvedObjectName string    `json:"involvedObjectName"`
	ObjectKind         string    `json:"objectKind"`
	Reason             string    `json:"reason"`
	Message            string    `json:"message"`
	Type               string    `json:"type"`
	Timestamp          time.Time `json:"timestamp"`
}

type PodContext struct {
	Name     string      `json:"name"`
	Status   string      `json:"status"`
	Ready    bool        `json:"ready"`
	Restarts int32       `json:"restarts"`
	Logs     string      `json:"logs"`
	Events   []EventInfo `json:"events"`
}

func getKubernetesClient() (*kubernetes.Clientset, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	kubeconfig := filepath.Join(homeDir, ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}

func podInfoFromPod(pod corev1.Pod) PodInfo {
	info := PodInfo{
		Name:   pod.Name,
		Status: string(pod.Status.Phase),
	}

	if len(pod.Status.ContainerStatuses) > 0 {
		info.Ready = pod.Status.ContainerStatuses[0].Ready
		info.Restarts = pod.Status.ContainerStatuses[0].RestartCount
	}

	return info
}

func fetchPodLogs(ctx context.Context, clientset kubernetes.Interface, podName string) ([]byte, error) {
	tailLines := int64(100)
	return clientset.CoreV1().Pods("default").
		GetLogs(podName, &corev1.PodLogOptions{TailLines: &tailLines}).
		DoRaw(ctx)
}

func fetchEvents(ctx context.Context, clientset kubernetes.Interface, podName string) ([]EventInfo, error) {
	events, err := clientset.CoreV1().Events("default").List(
		ctx,
		metav1.ListOptions{},
	)
	if err != nil {
		return nil, err
	}

	result := make([]EventInfo, 0, len(events.Items))
	for _, event := range events.Items {
		if podName != "" && event.InvolvedObject.Name != podName {
			continue
		}

		timestamp := event.EventTime.Time
		if event.Series != nil && event.Series.LastObservedTime.Time.After(timestamp) {
			timestamp = event.Series.LastObservedTime.Time
		}
		if event.LastTimestamp.Time.After(timestamp) {
			timestamp = event.LastTimestamp.Time
		}
		if timestamp.IsZero() {
			timestamp = event.CreationTimestamp.Time
		}

		result = append(result, EventInfo{
			InvolvedObjectName: event.InvolvedObject.Name,
			ObjectKind:         event.InvolvedObject.Kind,
			Reason:             event.Reason,
			Message:            event.Message,
			Type:               event.Type,
			Timestamp:          timestamp,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.After(result[j].Timestamp)
	})

	return result, nil
}

func podsHandler(w http.ResponseWriter, r *http.Request) {
	clientset, err := getKubernetesClient()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pods, err := clientset.CoreV1().Pods("default").List(
		context.Background(),
		metav1.ListOptions{},
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var result []PodInfo

	for _, pod := range pods.Items {
		result = append(result, podInfoFromPod(pod))
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func logsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	podName := strings.TrimSpace(r.URL.Query().Get("pod"))
	if podName == "" {
		http.Error(w, `missing required "pod" query parameter`, http.StatusBadRequest)
		return
	}

	clientset, err := getKubernetesClient()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create Kubernetes client: %v", err), http.StatusInternalServerError)
		return
	}

	logs, err := fetchPodLogs(r.Context(), clientset, podName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, fmt.Sprintf("pod %q was not found in the default namespace", podName), http.StatusNotFound)
			return
		}

		http.Error(w, fmt.Sprintf("failed to fetch logs for pod %q: %v", podName, err), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(logs)
}

func eventsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	podName := ""
	query := r.URL.Query()
	if query.Has("pod") {
		podName = strings.TrimSpace(query.Get("pod"))
		if podName == "" {
			http.Error(w, `"pod" query parameter cannot be empty`, http.StatusBadRequest)
			return
		}
	}

	clientset, err := getKubernetesClient()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create Kubernetes client: %v", err), http.StatusInternalServerError)
		return
	}

	result, err := fetchEvents(r.Context(), clientset, podName)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to fetch Kubernetes events: %v", err), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func contextHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	podName := strings.TrimSpace(r.URL.Query().Get("pod"))
	if podName == "" {
		http.Error(w, `missing required "pod" query parameter`, http.StatusBadRequest)
		return
	}

	clientset, err := getKubernetesClient()
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to create Kubernetes client: %v", err), http.StatusInternalServerError)
		return
	}

	pod, err := clientset.CoreV1().Pods("default").Get(
		r.Context(),
		podName,
		metav1.GetOptions{},
	)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, fmt.Sprintf("pod %q was not found in the default namespace", podName), http.StatusNotFound)
			return
		}

		http.Error(w, fmt.Sprintf("failed to fetch pod %q: %v", podName, err), http.StatusBadGateway)
		return
	}

	logs, err := fetchPodLogs(r.Context(), clientset, podName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, fmt.Sprintf("pod %q was not found in the default namespace", podName), http.StatusNotFound)
			return
		}

		http.Error(w, fmt.Sprintf("failed to fetch logs for pod %q: %v", podName, err), http.StatusBadGateway)
		return
	}

	events, err := fetchEvents(r.Context(), clientset, podName)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to fetch events for pod %q: %v", podName, err), http.StatusBadGateway)
		return
	}

	info := podInfoFromPod(*pod)
	result := PodContext{
		Name:     info.Name,
		Status:   info.Status,
		Ready:    info.Ready,
		Restarts: info.Restarts,
		Logs:     string(logs),
		Events:   events,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OpsLens API is running")
	})

	http.HandleFunc("/api/pods", podsHandler)
	http.HandleFunc("/api/logs", logsHandler)
	http.HandleFunc("/api/events", eventsHandler)
	http.HandleFunc("/api/context", contextHandler)

	fmt.Println("OpsLens API listening on port 8081")
	http.ListenAndServe(":8081", nil)
}
