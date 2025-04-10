package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func (a *PVCAutoscaler) reconcile(ctx context.Context) error {
	pvcl, err := getAnnotatedPVCs(ctx, a.kubeClient)
	if err != nil {
		return fmt.Errorf("could not get PersistentVolumeClaims: %w", err)
	}
	a.logger.WithField("count", pvcl.Size()).Debug("fetched annotated pvcs")

	pvcsMetrics, err := a.metricsClient.FetchPVCsMetrics(ctx, time.Now())
	if err != nil {
		a.logger.WithField("error", err).Error("could not fetch the PersistentVolumeClaims metrics")
		return nil
	}
	a.logger.Debug("fetched pvc metrics")

	for _, pvc := range pvcl.Items {
		pvcId := fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
		a.logger.WithField("id", pvcId).Debug("processing pvc")

		// Determine if the StorageClass allows volume expansion
		storageClassName := *pvc.Spec.StorageClassName
		isExpandable, err := isStorageClassExpandable(ctx, a.kubeClient, storageClassName)
		if err != nil {
			a.logger.WithFields(log.Fields{
				"storageClassName": storageClassName,
				"pvcId":            pvcId,
				"error":            err,
			}).Error("failed getting StorageClass")
			continue
		}
		if !isExpandable {
			a.logger.WithFields(log.Fields{
				"storageClassName": storageClassName,
				"pvcId":            pvcId,
			}).Error("the StorageClass does not allow volume expansion")
			continue
		}
		a.logger.WithField("id", pvcId).Debug("storageclass allows volume expansion")

		// Determine if pvc the meets the condition for resize
		err = isPVCResizable(&pvc)
		if err != nil {
			a.logger.WithFields(log.Fields{
				"pvcId": pvcId,
				"error": err,
			}).Error("the PersistentVolumeClaim %s is not resizable: %v")
			continue
		}
		a.logger.WithField("id", pvcId).Debug("pvc meets the resizing conditions")

		namespacedName := types.NamespacedName{
			Namespace: pvc.Namespace,
			Name:      pvc.Name,
		}
		if _, ok := pvcsMetrics[namespacedName]; !ok {
			a.logger.WithField("id", pvcId).Error("could not fetch the metrics for pvc")
			continue
		}
		a.logger.WithField("id", pvcId).Debug("metrics for pvc received")

		pvcCurrentCapacityBytes := pvcsMetrics[namespacedName].VolumeCapacityBytes

		threshold, err := convertPercentageToBytes(pvc.Annotations[PVCAutoscalerThresholdAnnotation], pvcCurrentCapacityBytes, DefaultThreshold)
		if err != nil {
			a.logger.WithFields(log.Fields{
				"pvcId": pvcId,
				"error": err,
			}).Error("failed to convert threshold annotation")
			continue
		}

		capacity, exists := pvc.Status.Capacity[corev1.ResourceStorage]
		if !exists {
			a.logger.WithField("id", pvcId).Info("skip pvc because its capacity is not set yet")
			continue
		}
		if capacity.Value() == 0 {
			a.logger.WithField("id", pvcId).Info("skip pvc because its capacity is zero")
			continue
		}

		increase, err := convertPercentageToBytes(pvc.Annotations[PVCAutoscalerIncreaseAnnotation], capacity.Value(), DefaultIncrease)
		if err != nil {
			a.logger.WithFields(log.Fields{
				"pvcId": pvcId,
				"error": err,
			}).Error("failed to convert increase annotation")
			continue
		}

		previousCapacity, exist := pvc.Annotations[PVCAutoscalerPreviousCapacityAnnotation]
		if exist {
			parsedPreviousCapacity, err := strconv.ParseInt(previousCapacity, 10, 64)
			if err != nil {
				a.logger.WithField("error", err).Error("failed to parse 'previous_capacity' annotation")
				continue
			}
			if parsedPreviousCapacity == pvcCurrentCapacityBytes {
				a.logger.WithField("id", pvcId).Info("pvc is still waiting to accept the resize")
				continue
			}
		}

		ceiling, err := getPVCStorageCeiling(&pvc)
		if err != nil {
			a.logger.WithFields(log.Fields{
				"pvcId": pvcId,
				"error": err,
			}).Error("failed to fetch storage ceiling")
			continue
		}
		if capacity.Cmp(ceiling) >= 0 {
			a.logger.WithFields(log.Fields{
				"pvcId": pvcId,
				"limit": ceiling.String(),
			}).Info("volume storage limit reached")
			continue
		}

		currentUsedBytes := pvcsMetrics[namespacedName].VolumeUsedBytes
		if currentUsedBytes >= threshold {
			a.logger.WithField("id", pvcId).Info("pvc usage bigger than threshold")

			// 1<<30 is a bit shift operation that represents 2^30, i.e. 1Gi
			newStorageBytes := int64(math.Ceil(float64(capacity.Value()+increase)/(1<<30))) << 30
			newStorage := resource.NewQuantity(newStorageBytes, resource.BinarySI)
			if newStorage.Cmp(ceiling) > 0 {
				newStorage = &ceiling
			}

			err := a.updatePVCWithNewStorageSize(ctx, &pvc, pvcCurrentCapacityBytes, newStorage)
			if err != nil {
				a.logger.WithFields(log.Fields{
					"pvcId": pvcId,
					"error": err,
				}).Error("failed to resize pvc")
			}

			a.logger.WithFields(log.Fields{
				"pvcId": pvcId,
				"old":   capacity.Value(),
				"new":   newStorage.Value(),
			}).Info("pvc resized")
		}
	}

	return nil
}

func (a *PVCAutoscaler) updatePVCWithNewStorageSize(ctx context.Context, pvcToResize *corev1.PersistentVolumeClaim, capacityBytes int64, newStorageBytes *resource.Quantity) error {
	pvcId := fmt.Sprintf("%s/%s", pvcToResize.Namespace, pvcToResize.Name)

	pvcToResize.Spec.Resources.Requests[corev1.ResourceStorage] = *newStorageBytes

	pvcToResize.Annotations[PVCAutoscalerPreviousCapacityAnnotation] = strconv.FormatInt(capacityBytes, 10)
	a.logger.WithField("id", pvcId).Debug("PVCAutoscalerPreviousCapacityAnnotation annotation written")

	_, err := a.kubeClient.CoreV1().PersistentVolumeClaims(pvcToResize.Namespace).Update(ctx, pvcToResize, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update PVC %s: %w", pvcId, err)
	}
	a.logger.WithField("id", pvcId).Debug("update function called and returned no error")

	return nil
}
