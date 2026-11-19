package org.infinispan.commons.logging;
public interface Log {
    boolean isTraceEnabled();
    void tracef(String fmt, Object... args);
}
