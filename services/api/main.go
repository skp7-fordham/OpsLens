package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

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
		ready := false
		var restarts int32

		if len(pod.Status.ContainerStatuses) > 0 {
			ready = pod.Status.ContainerStatuses[0].Ready
			restarts = pod.Status.ContainerStatuses[0].RestartCount
		}

		result = append(result, PodInfo{
			Name:     pod.Name,
			Status:   string(pod.Status.Phase),
			Ready:    ready,
			Restarts: restarts,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func main() {
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "OpsLens API is running")
	})

	http.HandleFunc("/api/pods", podsHandler)

	fmt.Println("OpsLens API listening on port 8081")
	http.ListenAndServe(":8081", nil)
}