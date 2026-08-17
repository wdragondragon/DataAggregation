package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferItemStatus;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class TransferItemStateMachine {

    private static final Map<TransferItemStatus, Set<TransferItemStatus>> TRANSITIONS = transitions();

    private final AtomicReference<TransferItemStatus> status;

    public TransferItemStateMachine(TransferItemStatus initialStatus) {
        status = new AtomicReference<>(initialStatus);
    }

    public TransferItemStatus status() {
        return status.get();
    }

    public TransferItemStatus transitionTo(TransferItemStatus next) {
        while (true) {
            TransferItemStatus current = status.get();
            if (current == next) {
                return current;
            }
            if (!TRANSITIONS.getOrDefault(current, Set.of()).contains(next)) {
                throw new IllegalStateException("illegal transfer item transition: " + current + " -> " + next);
            }
            if (status.compareAndSet(current, next)) {
                return current;
            }
        }
    }

    private static Map<TransferItemStatus, Set<TransferItemStatus>> transitions() {
        Map<TransferItemStatus, Set<TransferItemStatus>> values = new EnumMap<>(TransferItemStatus.class);
        values.put(TransferItemStatus.DISCOVERING,
                EnumSet.of(TransferItemStatus.QUEUED, TransferItemStatus.FAILED, TransferItemStatus.CANCELED));
        values.put(TransferItemStatus.QUEUED,
                EnumSet.of(TransferItemStatus.TRANSFERRING, TransferItemStatus.PAUSED,
                        TransferItemStatus.SKIPPED, TransferItemStatus.CONFLICT,
                        TransferItemStatus.FAILED, TransferItemStatus.CANCELED));
        values.put(TransferItemStatus.TRANSFERRING,
                EnumSet.of(TransferItemStatus.PAUSED, TransferItemStatus.VERIFYING,
                        TransferItemStatus.FAILED, TransferItemStatus.CANCELED));
        values.put(TransferItemStatus.PAUSED,
                EnumSet.of(TransferItemStatus.TRANSFERRING, TransferItemStatus.FAILED,
                        TransferItemStatus.CANCELED));
        values.put(TransferItemStatus.VERIFYING,
                EnumSet.of(TransferItemStatus.COMMITTING, TransferItemStatus.FAILED,
                        TransferItemStatus.CANCELED));
        values.put(TransferItemStatus.COMMITTING,
                EnumSet.of(TransferItemStatus.POST_ACTION, TransferItemStatus.SUCCESS,
                        TransferItemStatus.FAILED, TransferItemStatus.CANCELED));
        values.put(TransferItemStatus.POST_ACTION,
                EnumSet.of(TransferItemStatus.SUCCESS, TransferItemStatus.POST_ACTION_FAILED));
        values.put(TransferItemStatus.POST_ACTION_FAILED, EnumSet.of(TransferItemStatus.POST_ACTION));
        values.put(TransferItemStatus.FAILED,
                EnumSet.of(TransferItemStatus.QUEUED, TransferItemStatus.CANCELED));
        return Map.copyOf(values);
    }
}
