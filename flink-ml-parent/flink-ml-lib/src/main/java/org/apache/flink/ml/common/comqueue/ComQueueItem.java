package org.apache.flink.ml.common.comqueue;

import java.io.Serializable;

/**
 * Basic build block in {@link BaseComQueue}, for either communication or computation.
 */
public interface ComQueueItem extends Serializable {}
