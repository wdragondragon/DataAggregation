package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;

public interface ConflictResolver {
    
    boolean canResolve(DifferenceRecord differenceRecord);
    
    ResolutionResult resolve(DifferenceRecord differenceRecord);
    
    ConflictResolutionStrategy getStrategy();
}